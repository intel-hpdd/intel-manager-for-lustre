// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use futures::TryFutureExt;
use iml_cmd::{CheckedCommandExt, Command};
use iml_timer::config::{
    delete_config, get_config, service_file, timer_file, unit_name, write_configs,
};
use iml_tracing::tracing;
use warp::{self, http::StatusCode, Filter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    iml_tracing::init();

    // Match a config route
    let config_route = warp::put()
        .and(warp::path("configure"))
        .and(warp::body::content_length_limit(1024 * 16))
        .and(warp::body::json())
        .map(get_config)
        .and_then(write_configs)
        .and_then(move |(file_prefix, config_id)| async move {
            Command::new("systemctl")
                .arg("daemon-reload")
                .checked_output()
                .map_err(warp::reject::custom)
                .await?;

            Ok::<_, warp::Rejection>((file_prefix, config_id))
        })
        .and_then(
            move |(file_prefix, config_id): (String, String)| async move {
                let timer_path = format!(
                    "{}.timer",
                    unit_name(file_prefix.as_str(), config_id.as_str())
                );

                Command::new("systemctl")
                    .arg("enable")
                    .arg("--now")
                    .arg(timer_path)
                    .checked_output()
                    .map_err(warp::reject::custom)
                    .await
            },
        )
        .map(|_| Ok(StatusCode::CREATED));

    let unconfigure_route = warp::delete()
        .and(warp::path("unconfigure"))
        .and(warp::path::param::<String>())
        .and(warp::path::param::<String>())
        .and_then(move |file_prefix: String, config_id: String| async move {
            let timer_path = format!(
                "{}.timer",
                unit_name(file_prefix.as_str(), config_id.as_str())
            );

            Command::new("systemctl")
                .arg("disable")
                .arg("--now")
                .arg(timer_path)
                .checked_output()
                .map_err(warp::reject::custom)
                .await?;

            Ok::<_, warp::Rejection>((file_prefix, config_id))
        })
        .and_then(
            move |(file_prefix, config_id): (String, String)| async move {
                let timer_path = timer_file(&file_prefix, &config_id);
                delete_config(&timer_path, &file_prefix, &config_id).await
            },
        )
        .and_then(
            move |(file_prefix, config_id): (String, String)| async move {
                let timer_path = service_file(&file_prefix, &config_id);
                delete_config(&timer_path, &file_prefix, &config_id).await
            },
        )
        .and_then(move |_| async move {
            Command::new("systemctl")
                .arg("daemon-reload")
                .checked_output()
                .map_err(warp::reject::custom)
                .await
        })
        .map(|_| Ok(StatusCode::NO_CONTENT));

    let log = warp::log("iml_timer::api");
    let routes = config_route.or(unconfigure_route).with(log);

    tracing::debug!("Serving routes.");
    warp::serve(routes)
        .run(iml_manager_env::get_timer_addr())
        .await;

    Ok(())
}
