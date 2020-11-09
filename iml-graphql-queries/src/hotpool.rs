// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

pub mod create {
    use crate::Query;
    use iml_wire_types::Command;

    pub static QUERY: &str = r#"
            mutation CreateHotpool($fsname: String!, $hotpool: String!, $coldpool: String!,
                                   $minage: Int!, $freehi: Int!, $freelo: Int!,
                                   $extendlayout: String) {
              createHotpool(fsname: $fsname, hotpool: $hotpool, coldpool: $coldpool, minage: $minage,
                            freehi: $freehi, freelo: $freelo, extendlayout: $extendlayout) {
                cancelled
                complete
                created_at: createdAt
                errored
                id
                jobs
                logs
                message
                resource_uri: resourceUri
              }
            }
        "#;

    #[derive(Debug, serde::Serialize)]
    pub struct Vars {
        fsname: String,
        hotpool: String,
        coldpool: String,
        minage: i32,
        freehi: i32,
        freelo: i32,
        extendlayout: Option<String>,
    }

    pub fn build(
        fsname: impl ToString,
        hotpool: impl ToString,
        coldpool: impl ToString,
        minage: i32,
        freehi: i32,
        freelo: i32,
        extendlayout: Option<impl ToString>,
    ) -> Query<Vars> {
        Query {
            query: QUERY.to_string(),
            variables: Some(Vars {
                fsname: fsname.to_string(),
                hotpool: hotpool.to_string(),
                coldpool: coldpool.to_string(),
                freehi,
                freelo,
                minage,
                extendlayout: extendlayout.map(|x| x.to_string()),
            }),
        }
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct Resp {
        #[serde(rename(deserialize = "createHotpool"))]
        pub create_hotpool: Command,
    }
}

pub mod destroy {
    use crate::Query;
    use iml_wire_types::Command;

    pub static QUERY: &str = r#"
            mutation DestroyHotpool($fsname: String!) {
              destroyHotpool(fsname: $fsname) {
                cancelled
                complete
                created_at: createdAt
                errored
                id
                jobs
                logs
                message
                resource_uri: resourceUri
              }
            }
        "#;

    #[derive(Debug, serde::Serialize)]
    pub struct Vars {
        fsname: String,
    }

    pub fn build(fsname: impl ToString) -> Query<Vars> {
        Query {
            query: QUERY.to_string(),
            variables: Some(Vars {
                fsname: fsname.to_string(),
            }),
        }
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct Resp {
        #[serde(rename(deserialize = "destroyHotpool"))]
        pub destroy_hotpool: Command,
    }
}

pub mod list {
    use crate::Query;
    use iml_wire_types::{HotpoolConfiguration, SortDir};

    pub static QUERY: &str = r#"
            query Hotpools($dir: SortDir, $offset: Int, $limit: Int) {
              hotpools(dir: $dir, offset: $offset, limit: $limit) {
                id
                filesystem
                state
                state_modified_at: stateModifiedAt
                ha_label: haLabel
                version
                minage
                freehi
                freelo
                hot_id: hotId
                cold_id: coldId
                purge_id: purgeId
                resync_id: resyncId
                extend_id: extendId
              }
            }
        "#;

    #[derive(Debug, serde::Serialize)]
    pub struct Vars {
        dir: Option<SortDir>,
        offset: Option<u32>,
        limit: Option<u32>,
    }

    pub fn build(dir: Option<SortDir>, offset: Option<u32>, limit: Option<u32>) -> Query<Vars> {
        Query {
            query: QUERY.to_string(),
            variables: Some(Vars { dir, offset, limit }),
        }
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct Resp {
        #[serde(rename(deserialize = "hotpools"))]
        pub hotpools: Vec<HotpoolConfiguration>,
    }
}

pub mod start {
    use crate::Query;
    use iml_wire_types::Command;

    pub static QUERY: &str = r#"
            mutation StartHotpool($fsname: String!) {
              setHotpoolState(fsname: $fsname, state:STARTED) {
                cancelled
                complete
                created_at: createdAt
                errored
                id
                jobs
                logs
                message
                resource_uri: resourceUri
              }
            }
        "#;

    #[derive(Debug, serde::Serialize)]
    pub struct Vars {
        fsname: String,
    }

    pub fn build(fsname: impl ToString) -> Query<Vars> {
        Query {
            query: QUERY.to_string(),
            variables: Some(Vars {
                fsname: fsname.to_string(),
            }),
        }
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct Resp {
        #[serde(rename(deserialize = "setHotpoolState"))]
        pub start_hotpool: Command,
    }
}

pub mod stop {
    use crate::Query;
    use iml_wire_types::Command;

    pub static QUERY: &str = r#"
            mutation StopHotpool($fsname: String!) {
              setHotpoolState(fsname: $fsname, state:STOPPED) {
                cancelled
                complete
                created_at: createdAt
                errored
                id
                jobs
                logs
                message
                resource_uri: resourceUri
              }
            }
        "#;

    #[derive(Debug, serde::Serialize)]
    pub struct Vars {
        fsname: String,
    }

    pub fn build(fsname: impl ToString) -> Query<Vars> {
        Query {
            query: QUERY.to_string(),
            variables: Some(Vars {
                fsname: fsname.to_string(),
            }),
        }
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct Resp {
        #[serde(rename(deserialize = "setHotpoolState"))]
        pub stop_hotpool: Command,
    }
}
