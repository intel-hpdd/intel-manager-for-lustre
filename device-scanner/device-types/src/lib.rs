// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

#![allow(clippy::large_enum_variant)]

pub mod devices;
pub mod udev;
pub mod uevent;

use std::{
    cmp::Ordering,
    collections::BTreeSet,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct DevicePath(pub PathBuf);

impl<S: Into<PathBuf>> From<S> for DevicePath {
    fn from(s: S) -> DevicePath {
        DevicePath(s.into())
    }
}

impl<'a> From<&'a DevicePath> for &'a Path {
    fn from(s: &'a DevicePath) -> &'a Path {
        Path::new(&s.0)
    }
}

fn find_sort_slot(DevicePath(p): &DevicePath) -> usize {
    let o = &[
        Box::new(|p: &PathBuf| p.starts_with("/dev/mapper/")) as Box<dyn Fn(&PathBuf) -> bool>,
        Box::new(|p| p.starts_with("/dev/disk/by-id/")),
        Box::new(|p| p.starts_with("/dev/disk/by-path/")),
        Box::new(|p| p.starts_with("/dev/")),
        Box::new(|_| true),
    ]
    .iter()
    .position(|f| f(&p))
    .unwrap();

    *o
}

pub fn get_vdev_paths(vdev: &libzfs_types::VDev) -> BTreeSet<DevicePath> {
    match vdev {
        libzfs_types::VDev::Disk { dev_id, path, .. } => {
            let p = dev_id
                .as_ref()
                .map(|x| format!("/dev/disk/by-id/{}", x))
                .map(std::convert::Into::into)
                .or_else(|| {
                    tracing::warn!(
                        "VDev::Disk.dev_id not found, using VDev::Disk.path {:?}",
                        path
                    );

                    Some(path.clone())
                })
                .map(DevicePath);

            let mut b = BTreeSet::new();

            if let Some(x) = p {
                b.insert(x);
            }

            b
        }
        libzfs_types::VDev::File { .. } => BTreeSet::new(),
        libzfs_types::VDev::Mirror { children, .. }
        | libzfs_types::VDev::RaidZ { children, .. }
        | libzfs_types::VDev::Replacing { children, .. } => {
            children.iter().flat_map(get_vdev_paths).collect()
        }
        libzfs_types::VDev::Root {
            children,
            spares,
            cache,
            ..
        } => vec![children, spares, cache]
            .into_iter()
            .flatten()
            .flat_map(get_vdev_paths)
            .collect(),
    }
}

impl Ord for DevicePath {
    fn cmp(&self, other: &DevicePath) -> Ordering {
        let a_slot = find_sort_slot(self);
        let b_slot = find_sort_slot(other);

        match a_slot.cmp(&b_slot) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => self.0.partial_cmp(&other.0).unwrap(),
        }
    }
}

impl PartialOrd for DevicePath {
    fn partial_cmp(&self, other: &DevicePath) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DevicePath {
    fn eq(&self, other: &DevicePath) -> bool {
        self.0 == other.0
    }
}

impl Hash for DevicePath {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.0.as_path().hash(h)
    }
}

pub mod message {
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub enum Message {
        Data(String),
        Heartbeat,
    }
}

pub mod state {
    use crate::{mount, uevent};
    use im::{HashMap, HashSet};
    use std::path::PathBuf;

    pub type UEvents = HashMap<PathBuf, uevent::UEvent>;

    pub type ZedEvents = HashMap<u64, libzfs_types::Pool>;

    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
    pub struct State {
        pub uevents: UEvents,
        pub zed_events: ZedEvents,
        pub local_mounts: HashSet<mount::Mount>,
    }

    impl State {
        pub fn new() -> Self {
            State {
                uevents: HashMap::new(),
                zed_events: HashMap::new(),
                local_mounts: HashSet::new(),
            }
        }
    }
}

pub mod mount {
    use crate::DevicePath;
    use std::path::PathBuf;

    #[derive(
        Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
    )]
    #[serde(transparent)]
    pub struct MountPoint(pub PathBuf);

    #[derive(
        Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
    )]
    #[serde(transparent)]
    pub struct FsType(pub String);

    #[derive(
        Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
    )]
    pub struct MountOpts(pub String);

    #[derive(
        Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize, Clone,
    )]
    pub struct Mount {
        pub source: DevicePath,
        pub target: MountPoint,
        pub fs_type: FsType,
        pub opts: MountOpts,
    }

    impl Mount {
        pub fn new(
            target: MountPoint,
            source: DevicePath,
            fs_type: FsType,
            opts: MountOpts,
        ) -> Self {
            Mount {
                target,
                source,
                fs_type,
                opts,
            }
        }
    }

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum MountCommand {
        AddMount(MountPoint, DevicePath, FsType, MountOpts),
        RemoveMount(MountPoint, DevicePath, FsType, MountOpts),
        ReplaceMount(MountPoint, DevicePath, FsType, MountOpts, MountOpts),
        MoveMount(MountPoint, DevicePath, FsType, MountOpts, MountPoint),
    }
}

pub mod zed {

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum PoolCommand {
        AddPools(Vec<libzfs_types::Pool>),
        AddPool(libzfs_types::Pool),
        UpdatePool(libzfs_types::Pool),
        RemovePool(zpool::Guid),
        AddDataset(zpool::Guid, libzfs_types::Dataset),
        RemoveDataset(zpool::Guid, zfs::Name),
        SetZpoolProp(zpool::Guid, prop::Key, prop::Value),
        SetZfsProp(zpool::Guid, zfs::Name, prop::Key, prop::Value),
    }

    pub mod zpool {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Name(pub String);

        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Guid(pub String);

        impl From<u64> for Guid {
            fn from(x: u64) -> Self {
                Guid(format!("{:#018X}", x))
            }
        }

        impl From<Guid> for Result<u64, std::num::ParseIntError> {
            fn from(Guid(x): Guid) -> Self {
                let without_prefix = x.trim_start_matches("0x");
                u64::from_str_radix(without_prefix, 16)
            }
        }

        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct State(pub String);

        impl From<State> for String {
            fn from(State(x): State) -> Self {
                x
            }
        }
    }

    pub mod zfs {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Name(pub String);
    }

    pub mod prop {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Key(pub String);

        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct Value(pub String);
    }

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum ZedCommand {
        Init,
        CreateZpool(zpool::Name, zpool::Guid, zpool::State),
        ImportZpool(zpool::Name, zpool::Guid, zpool::State),
        ExportZpool(zpool::Guid, zpool::State),
        DestroyZpool(zpool::Guid),
        CreateZfs(zpool::Guid, zfs::Name),
        DestroyZfs(zpool::Guid, zfs::Name),
        SetZpoolProp(zpool::Guid, prop::Key, prop::Value),
        SetZfsProp(zpool::Guid, zfs::Name, prop::Key, prop::Value),
        AddVdev(zpool::Name, zpool::Guid),
    }
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Command {
    Stream,
    GetMounts,
    PoolCommand(zed::PoolCommand),
    UdevCommand(udev::UdevCommand),
    MountCommand(mount::MountCommand),
}

#[cfg(test)]
mod tests {
    use super::{
        mount, {Command, DevicePath},
    };
    use im::{ordset, OrdSet};
    use insta::assert_debug_snapshot;

    #[test]
    fn test_device_path_ordering() {
        let xs: OrdSet<DevicePath> = ordset![
            "/dev/disk/by-id/dm-uuid-part1-mpath-3600140550e41a841db244a992c31e7df".into(),
            "/dev/mapper/mpathd1".into(),
            "/dev/disk/by-uuid/b4550256-cf48-4013-8363-bfee5f52da12".into(),
            "/dev/disk/by-partuuid/d643e32f-b6b9-4863-af8f-8950376e28da".into(),
            "/dev/dm-20".into(),
            "/dev/disk/by-id/dm-name-mpathd1".into()
        ];

        assert_debug_snapshot!(xs);
    }

    #[test]
    fn test_mount_deserialize() {
        let s = "{\"MountCommand\":{\"AddMount\":[\"swap\",\"/dev/mapper/VolGroup00-LogVol01\",\"swap\",\"defaults\"]}}";

        let result = serde_json::from_str::<Command>(s).unwrap();

        assert_eq!(
            result,
            Command::MountCommand(mount::MountCommand::AddMount(
                mount::MountPoint("swap".into()),
                "/dev/mapper/VolGroup00-LogVol01".into(),
                mount::FsType("swap".to_string()),
                mount::MountOpts("defaults".to_string())
            ))
        )
    }
}
