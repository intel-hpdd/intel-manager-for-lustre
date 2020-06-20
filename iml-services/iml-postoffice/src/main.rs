// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use futures::TryStreamExt;
use iml_service_queue::service_queue::consume_data;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    iml_tracing::init();

    let pool = iml_rabbit::connect_to_rabbit(1);

    let conn = iml_rabbit::get_conn(pool).await?;

    let ch = iml_rabbit::create_channel(&conn).await?;

    let mut s = consume_data::<String>(&ch, "rust_agent_postoffice_rx");

    while let Some((fqdn, s)) = s.try_next().await? {
        tracing::info!("Got some postoffice data from {:?}: {:?}", fqdn, s);
    }

    Ok(())
}
