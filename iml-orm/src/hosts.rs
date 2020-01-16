// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

pub use crate::models::ChromaCoreManagedhost;
use crate::schema::chroma_core_managedhost as mh;
use diesel::{dsl, prelude::*};

pub type Table = mh::table;
pub type NotDeleted = dsl::Eq<mh::not_deleted, bool>;
pub type WithFqdn<'a> = dsl::And<dsl::Eq<mh::fqdn, &'a str>, NotDeleted>;
pub type ByFqdn<'a> = dsl::Filter<Table, WithFqdn<'a>>;

impl ChromaCoreManagedhost {
    pub fn all() -> Table {
        mh::table
    }
    pub fn not_deleted() -> NotDeleted {
        mh::not_deleted.eq(true)
    }
    pub fn with_fqdn(name: &str) -> WithFqdn<'_> {
        mh::fqdn.eq(name).and(Self::not_deleted())
    }
    pub fn by_fqdn<'a>(fqdn: &'a str) -> ByFqdn<'a> {
        Self::all().filter(Self::with_fqdn(fqdn))
    }
    pub fn is_setup(&self) -> bool {
        ["monitored", "managed", "working"]
            .iter()
            .find(|&x| x == &self.state)
            .is_some()
    }
}
