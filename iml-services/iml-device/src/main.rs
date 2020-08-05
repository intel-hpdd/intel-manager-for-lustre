// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use device_types::{devices::Device, mount::Mount};
use futures::{TryFutureExt, TryStreamExt};
use im::HashSet;
use iml_device::{
    build_device_index, client_mount_content_id, create_cache, create_target_cache, find_targets,
    linux_plugin_transforms::{
        build_device_lookup, devtree2linuxoutput, get_shared_pools, populate_zpool, update_vgs,
        LinuxPluginData,
    },
    update_client_mounts, update_devices, Cache, ImlDeviceError,
};
use iml_postgres::{get_db_pool, sqlx};
use iml_service_queue::service_queue::consume_data;
use iml_tracing::tracing;
use iml_wire_types::Fqdn;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use warp::Filter;

#[tokio::main]
async fn main() -> Result<(), ImlDeviceError> {
    iml_tracing::init();

    let addr = iml_manager_env::get_device_aggregator_addr();

    let pool = get_db_pool(5).await?;

    sqlx::migrate!("../../migrations").run(&pool).await?;

    let cache = create_cache(&pool).await?;

    let cache2 = Arc::clone(&cache);
    let cache = warp::any().map(move || Arc::clone(&cache));

    let get = warp::get().and(cache).and_then(|cache: Cache| {
        async move {
            let cache = cache.lock().await;

            let mut xs: BTreeMap<&Fqdn, _> = cache
                .iter()
                .map(|(k, v)| {
                    let mut out = LinuxPluginData::default();

                    devtree2linuxoutput(&v, None, &mut out);

                    (k, out)
                })
                .collect();

            let (path_index, cluster_pools): (HashMap<&Fqdn, _>, HashMap<&Fqdn, _>) = cache
                .iter()
                .map(|(k, v)| {
                    let mut path_to_mm = BTreeMap::new();
                    let mut pools = BTreeMap::new();

                    build_device_lookup(v, &mut path_to_mm, &mut pools);

                    ((k, path_to_mm), (k, pools))
                })
                .unzip();

            for (&h, x) in xs.iter_mut() {
                let path_to_mm = &path_index[h];
                let shared_pools = get_shared_pools(&h, path_to_mm, &cluster_pools);

                for (a, b) in shared_pools {
                    populate_zpool(a, b, x);
                }
            }

            let xs: BTreeMap<&Fqdn, LinuxPluginData> = update_vgs(xs, &path_index);

            Ok::<_, ImlDeviceError>(warp::reply::json(&xs))
        }
        .map_err(warp::reject::custom)
    });

    tracing::info!("Server starting");

    let server = warp::serve(get.with(warp::log("devices"))).run(addr);

    tokio::spawn(server);

    let rabbit_pool = iml_rabbit::connect_to_rabbit(1);

    let conn = iml_rabbit::get_conn(rabbit_pool).await?;

    let ch = iml_rabbit::create_channel(&conn).await?;

    let mut s = consume_data::<(Device, HashSet<Mount>)>(&ch, "rust_agent_device_rx");

    let lustreclientmount_ct_id = client_mount_content_id(&pool).await?;

    let mut mount_cache = HashMap::new();
    let mut target_cache = create_target_cache(&pool).await?;

    while let Some((host, (devices, mounts))) = s.try_next().await? {
        update_devices(&pool, &host, &devices).await?;
        update_client_mounts(&pool, lustreclientmount_ct_id, &host, &mounts).await?;

        let mut device_cache = cache2.lock().await;
        device_cache.insert(host.clone(), devices);
        mount_cache.insert(host, mounts);

        let index = build_device_index(&device_cache);

        let host_ids: HashMap<Fqdn, i32> =
            sqlx::query!("select fqdn, id from chroma_core_managedhost where not_deleted = 't'",)
                .fetch(&pool)
                .map_ok(|x| (Fqdn(x.fqdn), x.id))
                .try_collect()
                .await?;

        let targets = find_targets(&device_cache, &mount_cache, &host_ids, &index);
        targets.update_cache(&mut target_cache);
        targets.update_target_mounts_in_cache(&mut target_cache);

        let x = target_cache.0.clone().into_iter().fold(
            (vec![], vec![], vec![], vec![], vec![], vec![]),
            |mut acc, x| {
                acc.0.push(x.state);
                acc.1.push(x.name);
                acc.2.push(x.active_host_id);
                acc.3.push(
                    x.host_ids
                        .into_iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<String>>()
                        .join(","),
                );
                acc.4.push(x.uuid);
                acc.5.push(x.mount_path);

                acc
            },
        );

        tracing::warn!("x: {:?}", x);

        sqlx::query!(r#"INSERT INTO chroma_core_targets 
                        (state, name, active_host_id, host_ids, uuid, mount_path) 
                        SELECT state, name, active_host_id, string_to_array(host_ids, ',')::int[], uuid, mount_path
                        FROM UNNEST($1::text[], $2::text[], $3::int[], $4::text[], $5::text[], $6::text[])
                        AS t(state, name, active_host_id, host_ids, uuid, mount_path)
                        ON CONFLICT (uuid)
                            DO
                            UPDATE SET  state          = EXCLUDED.state,
                                        name           = EXCLUDED.name,
                                        active_host_id = EXCLUDED.active_host_id,
                                        host_ids       = EXCLUDED.host_ids,
                                        mount_path     = EXCLUDED.mount_path"#,
            &x.0,
            &x.1,
            &x.2 as &[Option<i32>],
            &x.3,
            &x.4,
            &x.5 as &[Option<String>],
        )
        .execute(&pool)
        .await?;
    }

    Ok(())
}
