// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::CompositeId;
use crate::ToCompositeId;
use crate::{EndpointName, Fqdn, Label};
use chrono::{offset::Utc, DateTime};
pub use iml_orm::sfa::{EnclosureType, HealthState, JobState, JobType, MemberState, SubTargetType};
use std::{collections::BTreeSet, fmt, ops::Deref, path::PathBuf};

#[cfg(feature = "postgres-interop")]
use bytes::BytesMut;
#[cfg(feature = "postgres-interop")]
use postgres_types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
#[cfg(feature = "postgres-interop")]
use std::{convert::TryInto, io, str::FromStr};
#[cfg(feature = "postgres-interop")]
use tokio_postgres::Row;

pub trait Id {
    /// Returns the `Id` (`i32`).
    fn id(&self) -> i32;
}

pub trait NotDeleted {
    /// Returns if the record is not deleted.
    fn not_deleted(&self) -> bool;
    /// Returns if the record is deleted.
    fn deleted(&self) -> bool {
        !self.not_deleted()
    }
}

fn not_deleted(x: Option<bool>) -> bool {
    x.filter(|&x| x).is_some()
}

/// The name of a `chroma` table
#[derive(serde::Deserialize, Debug, PartialEq, Eq)]
#[serde(transparent)]
pub struct TableName<'a>(pub &'a str);

impl fmt::Display for TableName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub trait Name {
    /// Get the name of a `chroma` table
    fn table_name() -> TableName<'static>;
}

/// Record from the `django_content_type` table
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct ContentTypeRecord {
    pub id: i32,
    pub app_label: String,
    pub model: String,
}

impl Id for ContentTypeRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

pub const CONTENT_TYPE_TABLE_NAME: TableName = TableName("django_content_type");

impl Name for ContentTypeRecord {
    fn table_name() -> TableName<'static> {
        CONTENT_TYPE_TABLE_NAME
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for ContentTypeRecord {
    fn from(row: Row) -> Self {
        ContentTypeRecord {
            id: row.get::<_, i32>("id"),
            app_label: row.get("app_label"),
            model: row.get("model"),
        }
    }
}

/// Record from the `lustre_fid` type
#[cfg(feature = "postgres-interop")]
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug, sqlx::Type)]
#[sqlx(rename = "lustre_fid")]
pub struct LustreFid {
    pub seq: i64,
    pub oid: i32,
    pub ver: i32,
}

#[cfg(feature = "postgres-interop")]
impl fmt::Display for LustreFid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[0x{:x}:0x{:x}:0x{:x}]",
            self.seq as u64, self.oid as u32, self.ver as u32
        )
    }
}

#[cfg(feature = "postgres-interop")]
impl FromStr for LustreFid {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let fidstr = s.trim_matches(|c| c == '[' || c == ']');
        let arr: Vec<&str> = fidstr
            .split(':')
            .map(|num| num.trim_start_matches("0x"))
            .collect();
        Ok(Self {
            seq: i64::from_str_radix(arr[0], 16)?,
            oid: i32::from_str_radix(arr[1], 16)?,
            ver: i32::from_str_radix(arr[2], 16)?,
        })
    }
}

/// Record from the `chroma_core_fidtaskqueue` table
#[cfg(feature = "postgres-interop")]
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct FidTaskQueue {
    pub id: i32,
    pub fid: LustreFid,
    pub data: serde_json::Value,
    pub task_id: i32,
}

/// Record from the `chroma_core_managedfilesystem` table
#[derive(serde::Deserialize, Debug)]
pub struct FsRecord {
    id: i32,
    state_modified_at: String,
    state: String,
    immutable_state: bool,
    name: String,
    mgs_id: i32,
    mdt_next_index: i32,
    ost_next_index: i32,
    not_deleted: Option<bool>,
    content_type_id: Option<i32>,
}

impl Id for FsRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

impl NotDeleted for FsRecord {
    fn not_deleted(&self) -> bool {
        not_deleted(self.not_deleted)
    }
}

pub const MANAGED_FILESYSTEM_TABLE_NAME: TableName = TableName("chroma_core_managedfilesystem");

impl Name for FsRecord {
    fn table_name() -> TableName<'static> {
        MANAGED_FILESYSTEM_TABLE_NAME
    }
}

/// Record from the `chroma_core_volume` table
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct VolumeRecord {
    pub id: i32,
    pub storage_resource_id: Option<i32>,
    pub size: Option<u64>,
    pub label: String,
    pub filesystem_type: Option<String>,
    pub not_deleted: Option<bool>,
    pub usable_for_lustre: bool,
}

impl Id for VolumeRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

impl NotDeleted for VolumeRecord {
    fn not_deleted(&self) -> bool {
        not_deleted(self.not_deleted)
    }
}

pub const VOLUME_TABLE_NAME: TableName = TableName("chroma_core_volume");

impl Name for VolumeRecord {
    fn table_name() -> TableName<'static> {
        VOLUME_TABLE_NAME
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for VolumeRecord {
    fn from(row: Row) -> Self {
        VolumeRecord {
            id: row.get::<_, i32>("id"),
            size: row.get::<_, Option<i64>>("size").map(|x| x as u64),
            label: row.get("label"),
            filesystem_type: row.get("filesystem_type"),
            usable_for_lustre: row.get("usable_for_lustre"),
            not_deleted: row.get("not_deleted"),
            storage_resource_id: row.get::<_, Option<i32>>("storage_resource_id"),
        }
    }
}

/// Record from the `chroma_core_volumenode` table
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct VolumeNodeRecord {
    pub id: i32,
    pub volume_id: i32,
    pub host_id: i32,
    pub path: String,
    pub storage_resource_id: Option<i32>,
    pub primary: bool,
    #[serde(rename = "use")]
    pub _use: bool,
    pub not_deleted: Option<bool>,
}

impl Id for VolumeNodeRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

impl Id for &VolumeNodeRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

impl NotDeleted for VolumeNodeRecord {
    fn not_deleted(&self) -> bool {
        not_deleted(self.not_deleted)
    }
}

impl Label for VolumeNodeRecord {
    fn label(&self) -> &str {
        &self.path
    }
}

impl Label for &VolumeNodeRecord {
    fn label(&self) -> &str {
        &self.path
    }
}

pub const VOLUME_NODE_TABLE_NAME: TableName = TableName("chroma_core_volumenode");

impl Name for VolumeNodeRecord {
    fn table_name() -> TableName<'static> {
        VOLUME_NODE_TABLE_NAME
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for VolumeNodeRecord {
    fn from(row: Row) -> Self {
        VolumeNodeRecord {
            id: row.get::<_, i32>("id"),
            volume_id: row.get::<_, i32>("volume_id"),
            host_id: row.get::<_, i32>("host_id"),
            path: row.get("path"),
            storage_resource_id: row.get::<_, Option<i32>>("storage_resource_id"),
            primary: row.get("primary"),
            _use: row.get("use"),
            not_deleted: row.get("not_deleted"),
        }
    }
}

/// Record from the `chroma_core_managedtargetmount` table
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct ManagedTargetMountRecord {
    pub id: i32,
    pub host_id: i32,
    pub mount_point: Option<String>,
    pub volume_node_id: i32,
    pub primary: bool,
    pub target_id: i32,
    pub not_deleted: Option<bool>,
}

impl Id for ManagedTargetMountRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

impl NotDeleted for ManagedTargetMountRecord {
    fn not_deleted(&self) -> bool {
        not_deleted(self.not_deleted)
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for ManagedTargetMountRecord {
    fn from(row: Row) -> Self {
        ManagedTargetMountRecord {
            id: row.get::<_, i32>("id"),
            host_id: row.get::<_, i32>("host_id"),
            mount_point: row.get("mount_point"),
            volume_node_id: row.get::<_, i32>("volume_node_id"),
            primary: row.get("primary"),
            target_id: row.get::<_, i32>("target_id"),
            not_deleted: row.get("not_deleted"),
        }
    }
}

pub const MANAGED_TARGET_MOUNT_TABLE_NAME: TableName = TableName("chroma_core_managedtargetmount");

impl Name for ManagedTargetMountRecord {
    fn table_name() -> TableName<'static> {
        MANAGED_TARGET_MOUNT_TABLE_NAME
    }
}

/// Record from the `chroma_core_managedtarget` table
#[derive(serde::Deserialize, Debug)]
pub struct ManagedTargetRecord {
    id: i32,
    state_modified_at: String,
    state: String,
    immutable_state: bool,
    name: Option<String>,
    uuid: Option<String>,
    ha_label: Option<String>,
    volume_id: i32,
    inode_size: Option<i32>,
    bytes_per_inode: Option<i32>,
    inode_count: Option<u64>,
    reformat: bool,
    active_mount_id: Option<i32>,
    not_deleted: Option<bool>,
    content_type_id: Option<i32>,
}

impl Id for ManagedTargetRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

impl NotDeleted for ManagedTargetRecord {
    fn not_deleted(&self) -> bool {
        not_deleted(self.not_deleted)
    }
}

pub const MANAGED_TARGET_TABLE_NAME: TableName = TableName("chroma_core_managedtarget");

impl Name for ManagedTargetRecord {
    fn table_name() -> TableName<'static> {
        MANAGED_TARGET_TABLE_NAME
    }
}

/// Record from the `chroma_core_ostpool` table
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct OstPoolRecord {
    pub id: i32,
    pub name: String,
    pub filesystem_id: i32,
    pub not_deleted: Option<bool>,
    pub content_type_id: Option<i32>,
}

impl Id for OstPoolRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

impl Id for &OstPoolRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

impl NotDeleted for OstPoolRecord {
    fn not_deleted(&self) -> bool {
        not_deleted(self.not_deleted)
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for OstPoolRecord {
    fn from(row: Row) -> Self {
        OstPoolRecord {
            id: row.get::<_, i32>("id"),
            name: row.get("name"),
            filesystem_id: row.get::<_, i32>("filesystem_id"),
            not_deleted: row.get("not_deleted"),
            content_type_id: row.get::<_, Option<i32>>("content_type_id"),
        }
    }
}

pub const OSTPOOL_TABLE_NAME: TableName = TableName("chroma_core_ostpool");

impl Name for OstPoolRecord {
    fn table_name() -> TableName<'static> {
        OSTPOOL_TABLE_NAME
    }
}

impl Label for OstPoolRecord {
    fn label(&self) -> &str {
        &self.name
    }
}

impl Label for &OstPoolRecord {
    fn label(&self) -> &str {
        &self.name
    }
}

/// Record from the `chroma_core_ostpool_osts` table
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct OstPoolOstsRecord {
    pub id: i32,
    pub ostpool_id: i32,
    pub managedost_id: i32,
}

impl Id for OstPoolOstsRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for OstPoolOstsRecord {
    fn from(row: Row) -> Self {
        OstPoolOstsRecord {
            id: row.get::<_, i32>("id"),
            ostpool_id: row.get::<_, i32>("ostpool_id"),
            managedost_id: row.get::<_, i32>("managedost_id"),
        }
    }
}

pub const OSTPOOL_OSTS_TABLE_NAME: TableName = TableName("chroma_core_ostpool_osts");

impl Name for OstPoolOstsRecord {
    fn table_name() -> TableName<'static> {
        OSTPOOL_OSTS_TABLE_NAME
    }
}

/// Record from the `chroma_core_managedost` table
#[derive(serde::Deserialize, Debug)]
pub struct ManagedOstRecord {
    managedtarget_ptr_id: i32,
    index: i32,
    filesystem_id: i32,
}

impl Id for ManagedOstRecord {
    fn id(&self) -> i32 {
        self.managedtarget_ptr_id
    }
}

impl NotDeleted for ManagedOstRecord {
    fn not_deleted(&self) -> bool {
        true
    }
}

pub const MANAGED_OST_TABLE_NAME: TableName = TableName("chroma_core_managedost");

impl Name for ManagedOstRecord {
    fn table_name() -> TableName<'static> {
        MANAGED_OST_TABLE_NAME
    }
}

/// Record from the `chroma_core_managedmdt` table
#[derive(serde::Deserialize, Debug)]
pub struct ManagedMdtRecord {
    managedtarget_ptr_id: i32,
    index: i32,
    filesystem_id: i32,
}

impl Id for ManagedMdtRecord {
    fn id(&self) -> i32 {
        self.managedtarget_ptr_id
    }
}

impl NotDeleted for ManagedMdtRecord {
    fn not_deleted(&self) -> bool {
        true
    }
}

pub const MANAGED_MDT_TABLE_NAME: TableName = TableName("chroma_core_managedmdt");

impl Name for ManagedMdtRecord {
    fn table_name() -> TableName<'static> {
        MANAGED_MDT_TABLE_NAME
    }
}

/// Record from the `chroma_core_managedhost` table
#[derive(serde::Deserialize, Debug)]
pub struct ManagedHostRecord {
    pub id: i32,
    pub state_modified_at: DateTime<Utc>,
    pub state: String,
    pub immutable_state: bool,
    pub not_deleted: Option<bool>,
    pub content_type_id: Option<i32>,
    pub address: String,
    pub fqdn: String,
    pub nodename: String,
    pub boot_time: Option<DateTime<Utc>>,
    pub server_profile_id: Option<String>,
    pub needs_update: bool,
    pub install_method: String,
    pub corosync_ring0: String,
}

impl Id for ManagedHostRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

impl NotDeleted for ManagedHostRecord {
    fn not_deleted(&self) -> bool {
        not_deleted(self.not_deleted)
    }
}

pub const MANAGED_HOST_TABLE_NAME: TableName = TableName("chroma_core_managedhost");

impl Name for ManagedHostRecord {
    fn table_name() -> TableName<'static> {
        MANAGED_HOST_TABLE_NAME
    }
}

impl ManagedHostRecord {
    pub fn is_setup(&self) -> bool {
        ["monitored", "managed", "working"]
            .iter()
            .any(|&x| x == self.state)
    }
}

/// Record from the `chroma_core_alertstate` table
#[derive(serde::Deserialize, Debug)]
pub struct AlertStateRecord {
    id: i32,
    alert_item_type_id: Option<i32>,
    alert_item_id: Option<i32>,
    alert_type: String,
    begin: String,
    end: Option<String>,
    active: Option<bool>,
    dismissed: bool,
    severity: i32,
    record_type: String,
    variant: Option<String>,
    lustre_pid: Option<i32>,
    message: Option<String>,
}

impl AlertStateRecord {
    pub fn is_active(&self) -> bool {
        self.active.is_some()
    }
}

impl Id for AlertStateRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

pub const ALERT_STATE_TABLE_NAME: TableName = TableName("chroma_core_alertstate");

impl Name for AlertStateRecord {
    fn table_name() -> TableName<'static> {
        ALERT_STATE_TABLE_NAME
    }
}

/// Record from the `chroma_core_stratagemconfiguration` table
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct StratagemConfiguration {
    pub id: i32,
    pub filesystem_id: i32,
    pub interval: u64,
    pub report_duration: Option<u64>,
    pub purge_duration: Option<u64>,
    pub immutable_state: bool,
    pub not_deleted: Option<bool>,
    pub state: String,
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for StratagemConfiguration {
    fn from(row: Row) -> Self {
        StratagemConfiguration {
            id: row.get::<_, i32>("id"),
            filesystem_id: row.get::<_, i32>("filesystem_id"),
            interval: row.get::<_, i64>("interval") as u64,
            report_duration: row
                .get::<_, Option<i64>>("report_duration")
                .map(|x| x as u64),
            purge_duration: row
                .get::<_, Option<i64>>("purge_duration")
                .map(|x| x as u64),
            immutable_state: row.get("immutable_state"),
            not_deleted: row.get("not_deleted"),
            state: row.get("state"),
        }
    }
}

impl Id for StratagemConfiguration {
    fn id(&self) -> i32 {
        self.id
    }
}

impl NotDeleted for StratagemConfiguration {
    fn not_deleted(&self) -> bool {
        not_deleted(self.not_deleted)
    }
}

pub const STRATAGEM_CONFIGURATION_TABLE_NAME: TableName =
    TableName("chroma_core_stratagemconfiguration");

impl Name for StratagemConfiguration {
    fn table_name() -> TableName<'static> {
        STRATAGEM_CONFIGURATION_TABLE_NAME
    }
}

impl Label for StratagemConfiguration {
    fn label(&self) -> &str {
        "Stratagem Configuration"
    }
}

impl EndpointName for StratagemConfiguration {
    fn endpoint_name() -> &'static str {
        "stratagem_configuration"
    }
}

/// Record from the `chroma_core_lnetconfiguration` table
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct LnetConfigurationRecord {
    pub id: i32,
    pub state: String,
    pub host_id: i32,
    pub immutable_state: bool,
    pub not_deleted: Option<bool>,
    pub content_type_id: Option<i32>,
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for LnetConfigurationRecord {
    fn from(row: Row) -> Self {
        LnetConfigurationRecord {
            id: row.get::<_, i32>("id"),
            state: row.get("state"),
            host_id: row.get::<_, i32>("host_id"),
            immutable_state: row.get("immutable_state"),
            not_deleted: row.get("not_deleted"),
            content_type_id: row.get::<_, Option<i32>>("content_type_id"),
        }
    }
}

impl Id for LnetConfigurationRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

impl NotDeleted for LnetConfigurationRecord {
    fn not_deleted(&self) -> bool {
        not_deleted(self.not_deleted)
    }
}

impl EndpointName for LnetConfigurationRecord {
    fn endpoint_name() -> &'static str {
        "lnet_configuration"
    }
}

impl Label for LnetConfigurationRecord {
    fn label(&self) -> &str {
        "lnet configuration"
    }
}

impl ToCompositeId for LnetConfigurationRecord {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id.unwrap(), self.id)
    }
}

impl ToCompositeId for &LnetConfigurationRecord {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id.unwrap(), self.id)
    }
}

pub const LNET_CONFIGURATION_TABLE_NAME: TableName = TableName("chroma_core_lnetconfiguration");

impl Name for LnetConfigurationRecord {
    fn table_name() -> TableName<'static> {
        LNET_CONFIGURATION_TABLE_NAME
    }
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq, Ord, PartialOrd, Clone, Hash,
)]
pub struct DeviceId(String);

#[cfg(feature = "postgres-interop")]
impl ToSql for DeviceId {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        <&str as ToSql>::to_sql(&&*self.0, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

#[cfg(feature = "postgres-interop")]
impl<'a> FromSql<'a> for DeviceId {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<DeviceId, Box<dyn std::error::Error + Sync + Send>> {
        FromSql::from_sql(ty, raw).map(DeviceId)
    }

    fn accepts(ty: &Type) -> bool {
        <String as FromSql>::accepts(ty)
    }
}

impl Deref for DeviceId {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct DeviceIds(pub BTreeSet<DeviceId>);

impl Deref for DeviceIds {
    type Target = BTreeSet<DeviceId>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "postgres-interop")]
impl ToSql for DeviceIds {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let xs = self.0.iter().collect::<Vec<_>>();
        <&[&DeviceId] as ToSql>::to_sql(&&*xs, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&[&DeviceId] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

#[cfg(feature = "postgres-interop")]
impl<'a> FromSql<'a> for DeviceIds {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<DeviceIds, Box<dyn std::error::Error + Sync + Send>> {
        <Vec<DeviceId> as FromSql>::from_sql(ty, raw).map(|xs| DeviceIds(xs.into_iter().collect()))
    }

    fn accepts(ty: &Type) -> bool {
        <Vec<DeviceId> as FromSql>::accepts(ty)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Size(pub u64);

#[cfg(feature = "postgres-interop")]
impl ToSql for Size {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        <&str as ToSql>::to_sql(&&*self.0.to_string(), ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

#[cfg(feature = "postgres-interop")]
impl<'a> FromSql<'a> for Size {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Size, Box<dyn std::error::Error + Sync + Send>> {
        <String as FromSql>::from_sql(ty, raw).and_then(|x| {
            x.parse::<u64>()
                .map(Size)
                .map_err(|e| -> Box<dyn std::error::Error + Sync + Send> { Box::new(e) })
        })
    }

    fn accepts(ty: &Type) -> bool {
        <String as FromSql>::accepts(ty)
    }
}

/// The current type of Devices we support
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub enum DeviceType {
    ScsiDevice,
    Partition,
    MdRaid,
    Mpath,
    VolumeGroup,
    LogicalVolume,
    Zpool,
    Dataset,
}

impl std::fmt::Display for DeviceType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ScsiDevice => write!(f, "scsi"),
            Self::Partition => write!(f, "partition"),
            Self::MdRaid => write!(f, "mdraid"),
            Self::Mpath => write!(f, "mpath"),
            Self::VolumeGroup => write!(f, "vg"),
            Self::LogicalVolume => write!(f, "lv"),
            Self::Zpool => write!(f, "zpool"),
            Self::Dataset => write!(f, "dataset"),
        }
    }
}

#[cfg(feature = "postgres-interop")]
impl ToSql for DeviceType {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        <String as ToSql>::to_sql(&format!("{}", self), ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <String as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

#[cfg(feature = "postgres-interop")]
impl<'a> FromSql<'a> for DeviceType {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<DeviceType, Box<dyn std::error::Error + Sync + Send>> {
        FromSql::from_sql(ty, raw).and_then(|x| match x {
            "scsi" => Ok(DeviceType::ScsiDevice),
            "partition" => Ok(DeviceType::Partition),
            "mdraid" => Ok(DeviceType::MdRaid),
            "mpath" => Ok(DeviceType::Mpath),
            "vg" => Ok(DeviceType::VolumeGroup),
            "lv" => Ok(DeviceType::LogicalVolume),
            "zpool" => Ok(DeviceType::Zpool),
            "dataset" => Ok(DeviceType::Dataset),
            _ => {
                let e: Box<dyn std::error::Error + Sync + Send> = Box::new(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Unknown DeviceType variant",
                ));

                Err(e)
            }
        })
    }

    fn accepts(ty: &Type) -> bool {
        <String as FromSql>::accepts(ty)
    }
}

/// A device (Block or Virtual).
/// These should be unique per cluster
#[derive(Debug, PartialEq, Eq)]
pub struct Device {
    pub id: DeviceId,
    pub size: Size,
    pub usable_for_lustre: bool,
    pub device_type: DeviceType,
    pub parents: DeviceIds,
    pub children: DeviceIds,
}

pub const DEVICE_TABLE_NAME: TableName = TableName("chroma_core_device");

impl Name for Device {
    fn table_name() -> TableName<'static> {
        DEVICE_TABLE_NAME
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for Device {
    fn from(row: Row) -> Self {
        Device {
            id: row.get("id"),
            size: row.get("size"),
            usable_for_lustre: row.get("usable_for_lustre"),
            device_type: row.get("device_type"),
            parents: row.get("parents"),
            children: row.get("children"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Paths(pub BTreeSet<PathBuf>);

impl Deref for Paths {
    type Target = BTreeSet<PathBuf>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "postgres-interop")]
impl ToSql for Paths {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let xs = self.iter().map(|x| x.to_string_lossy()).collect::<Vec<_>>();
        <&[std::borrow::Cow<'_, str>] as ToSql>::to_sql(&&*xs, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&[std::borrow::Cow<'_, str>] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

#[cfg(feature = "postgres-interop")]
impl<'a> FromSql<'a> for Paths {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Paths, Box<dyn std::error::Error + Sync + Send>> {
        <Vec<String> as FromSql>::from_sql(ty, raw)
            .map(|xs| Paths(xs.into_iter().map(PathBuf::from).collect()))
    }

    fn accepts(ty: &Type) -> bool {
        <Vec<String> as FromSql>::accepts(ty)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct MountPath(pub Option<PathBuf>);

#[cfg(feature = "postgres-interop")]
impl ToSql for MountPath {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        <&Option<String> as ToSql>::to_sql(
            &&self.0.clone().map(|x| x.to_string_lossy().into_owned()),
            ty,
            w,
        )
    }

    fn accepts(ty: &Type) -> bool {
        <&Option<String> as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

#[cfg(feature = "postgres-interop")]
impl Deref for MountPath {
    type Target = Option<PathBuf>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A pointer to a `Device` present on a host.
/// Stores mount_path and paths to reach the pointed to `Device`.
#[derive(Debug, PartialEq, Eq)]
pub struct DeviceHost {
    pub device_id: DeviceId,
    pub fqdn: Fqdn,
    pub local: bool,
    pub paths: Paths,
    pub mount_path: MountPath,
    pub fs_type: Option<String>,
    pub fs_label: Option<String>,
    pub fs_uuid: Option<String>,
}

pub const DEVICE_HOST_TABLE_NAME: TableName = TableName("chroma_core_devicehost");

impl Name for DeviceHost {
    fn table_name() -> TableName<'static> {
        DEVICE_HOST_TABLE_NAME
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for DeviceHost {
    fn from(row: Row) -> Self {
        DeviceHost {
            device_id: row.get("device_id"),
            fqdn: Fqdn(row.get::<_, String>("fqdn")),
            local: row.get("local"),
            paths: row.get("paths"),
            mount_path: MountPath(
                row.get::<_, Option<String>>("mount_path")
                    .map(PathBuf::from),
            ),
            fs_type: row.get::<_, Option<String>>("fs_type"),
            fs_label: row.get::<_, Option<String>>("fs_label"),
            fs_uuid: row.get::<_, Option<String>>("fs_uuid"),
        }
    }
}

/// Record from the `auth_user` table
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AuthUserRecord {
    pub id: i32,
    pub is_superuser: bool,
    pub username: String,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub is_staff: bool,
    pub is_active: bool,
}

pub const AUTH_USER_TABLE_NAME: TableName = TableName("auth_user");

impl Name for AuthUserRecord {
    fn table_name() -> TableName<'static> {
        AUTH_USER_TABLE_NAME
    }
}

impl Id for AuthUserRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for AuthUserRecord {
    fn from(row: Row) -> Self {
        Self {
            id: row.get::<_, i32>("id"),
            is_superuser: row.get("is_superuser"),
            username: row.get("username"),
            first_name: row.get("first_name"),
            last_name: row.get("last_name"),
            email: row.get("email"),
            is_staff: row.get("is_staff"),
            is_active: row.get("is_active"),
        }
    }
}

/// Record from the `auth_user_groups` table
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AuthUserGroupRecord {
    pub id: i32,
    pub user_id: i32,
    pub group_id: i32,
}

pub const AUTH_USER_GROUP_TABLE_NAME: TableName = TableName("auth_user_groups");

impl Name for AuthUserGroupRecord {
    fn table_name() -> TableName<'static> {
        AUTH_USER_GROUP_TABLE_NAME
    }
}

impl Id for AuthUserGroupRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for AuthUserGroupRecord {
    fn from(row: Row) -> Self {
        Self {
            id: row.get::<_, i32>("id"),
            user_id: row.get::<_, i32>("user_id"),
            group_id: row.get::<_, i32>("group_id"),
        }
    }
}

/// Record from the `auth_group` table
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AuthGroupRecord {
    pub id: i32,
    pub name: String,
}

pub const AUTH_GROUP_TABLE_NAME: TableName = TableName("auth_group");

impl Name for AuthGroupRecord {
    fn table_name() -> TableName<'static> {
        AUTH_GROUP_TABLE_NAME
    }
}

impl Id for AuthGroupRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for AuthGroupRecord {
    fn from(row: Row) -> Self {
        Self {
            id: row.get::<_, i32>("id"),
            name: row.get("name"),
        }
    }
}

/// Record from the `chroma_core_pacemakerconfiguration` table
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PacemakerConfigurationRecord {
    pub id: i32,
    pub state: String,
    pub immutable_state: bool,
    pub not_deleted: Option<bool>,
    pub content_type_id: Option<i32>,
    pub host_id: i32,
}

pub const PACEMAKER_CONFIGURATION_TABLE_NAME: TableName =
    TableName("chroma_core_pacemakerconfiguration");

impl Name for PacemakerConfigurationRecord {
    fn table_name() -> TableName<'static> {
        PACEMAKER_CONFIGURATION_TABLE_NAME
    }
}

impl Id for PacemakerConfigurationRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

impl NotDeleted for PacemakerConfigurationRecord {
    fn not_deleted(&self) -> bool {
        not_deleted(self.not_deleted)
    }
}

impl EndpointName for PacemakerConfigurationRecord {
    fn endpoint_name() -> &'static str {
        "pacemaker_configuration"
    }
}

impl Label for PacemakerConfigurationRecord {
    fn label(&self) -> &str {
        "pacemaker configuration"
    }
}

impl ToCompositeId for PacemakerConfigurationRecord {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id.unwrap(), self.id)
    }
}

impl ToCompositeId for &PacemakerConfigurationRecord {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id.unwrap(), self.id)
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for PacemakerConfigurationRecord {
    fn from(row: Row) -> Self {
        Self {
            id: row.get::<_, i32>("id"),
            state: row.get("state"),
            immutable_state: row.get("immutable_state"),
            not_deleted: row.get("not_deleted"),
            content_type_id: row.get::<_, Option<i32>>("content_type_id"),
            host_id: row.get::<_, i32>("host_id"),
        }
    }
}

/// Record from the `chroma_core_corosyncconfiguration` table
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CorosyncConfigurationRecord {
    pub id: i32,
    pub state: String,
    pub immutable_state: bool,
    pub not_deleted: Option<bool>,
    pub mcast_port: Option<i32>,
    pub corosync_reported_up: bool,
    pub content_type_id: Option<i32>,
    pub host_id: i32,
}

pub const COROSYNC_CONFIGURATION_TABLE_NAME: TableName =
    TableName("chroma_core_corosyncconfiguration");

impl Name for CorosyncConfigurationRecord {
    fn table_name() -> TableName<'static> {
        COROSYNC_CONFIGURATION_TABLE_NAME
    }
}

impl Id for CorosyncConfigurationRecord {
    fn id(&self) -> i32 {
        self.id
    }
}

impl NotDeleted for CorosyncConfigurationRecord {
    fn not_deleted(&self) -> bool {
        not_deleted(self.not_deleted)
    }
}

impl EndpointName for CorosyncConfigurationRecord {
    fn endpoint_name() -> &'static str {
        "corosync_configuration"
    }
}

impl Label for CorosyncConfigurationRecord {
    fn label(&self) -> &str {
        "corosync configuration"
    }
}

impl ToCompositeId for CorosyncConfigurationRecord {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id.unwrap(), self.id)
    }
}

impl ToCompositeId for &CorosyncConfigurationRecord {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id.unwrap(), self.id)
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for CorosyncConfigurationRecord {
    fn from(row: Row) -> Self {
        Self {
            id: row.get::<_, i32>("id"),
            state: row.get("state"),
            immutable_state: row.get("immutable_state"),
            not_deleted: row.get("not_deleted"),
            mcast_port: row.get::<_, Option<i32>>("mcast_port"),
            corosync_reported_up: row.get("corosync_reported_up"),
            content_type_id: row.get::<_, Option<i32>>("content_type_id"),
            host_id: row.get::<_, i32>("host_id"),
        }
    }
}

/// Record from the `chroma_core_sfastoragesystem` table
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SfaStorageSystem {
    pub id: i32,
    pub child_health_state: HealthState,
    pub health_state_reason: String,
    pub health_state: HealthState,
    pub uuid: String,
    pub platform: String,
}

pub const SFA_STORAGE_SYSTEM_TABLE_NAME: TableName = TableName("chroma_core_sfastoragesystem");

impl Name for SfaStorageSystem {
    fn table_name() -> TableName<'static> {
        SFA_STORAGE_SYSTEM_TABLE_NAME
    }
}

impl Id for SfaStorageSystem {
    fn id(&self) -> i32 {
        self.id
    }
}

impl Label for SfaStorageSystem {
    fn label(&self) -> &str {
        "SFA Storage System"
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for SfaStorageSystem {
    fn from(row: Row) -> Self {
        Self {
            id: row.get::<_, i32>("id"),
            child_health_state: row
                .get::<_, i16>("child_health_state")
                .try_into()
                .unwrap_or_default(),
            health_state_reason: row.get("health_state_reason"),
            health_state: row
                .get::<_, i16>("health_state")
                .try_into()
                .unwrap_or_default(),
            uuid: row.get("uuid"),
            platform: row.get("platform"),
        }
    }
}

pub const SFA_ENCLOSURE_TABLE_NAME: TableName = TableName("chroma_core_sfaenclosure");

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SfaEnclosure {
    pub id: i32,
    pub index: i32,
    pub element_name: String,
    pub health_state: HealthState,
    pub health_state_reason: String,
    pub child_health_state: HealthState,
    pub model: String,
    pub position: i16,
    pub enclosure_type: EnclosureType,
    pub canister_location: String,
    pub storage_system: String,
}

impl Name for SfaEnclosure {
    fn table_name() -> TableName<'static> {
        SFA_ENCLOSURE_TABLE_NAME
    }
}

impl Id for SfaEnclosure {
    fn id(&self) -> i32 {
        self.id
    }
}

impl Label for SfaEnclosure {
    fn label(&self) -> &str {
        "SFA enclosure"
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for SfaEnclosure {
    fn from(row: Row) -> Self {
        Self {
            id: row.get::<_, i32>("id"),
            index: row.get::<_, i32>("index"),
            element_name: row.get("element_name"),
            health_state: row
                .get::<_, i16>("health_state")
                .try_into()
                .unwrap_or_default(),
            health_state_reason: row.get("health_state_reason"),
            child_health_state: row
                .get::<_, i16>("child_health_state")
                .try_into()
                .unwrap_or_default(),
            model: row.get("model"),
            position: row.get::<_, i16>("position"),
            enclosure_type: row
                .get::<_, i16>("enclosure_type")
                .try_into()
                .unwrap_or_default(),
            storage_system: row.get("storage_system"),
            canister_location: row.get("canister_location"),
        }
    }
}

pub const SFA_DISK_DRIVE_TABLE_NAME: TableName = TableName("chroma_core_sfadiskdrive");

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SfaDiskDrive {
    pub id: i32,
    pub index: i32,
    pub enclosure_index: i32,
    pub failed: bool,
    pub slot_number: i32,
    pub health_state: HealthState,
    pub health_state_reason: String,
    /// Specifies the member index of the disk drive.
    /// If the disk drive is not a member of a pool, this value will be not be set.
    pub member_index: Option<i16>,
    /// Specifies the state of the disk drive relative to a containing pool.
    pub member_state: MemberState,
    pub storage_system: String,
}

impl Name for SfaDiskDrive {
    fn table_name() -> TableName<'static> {
        SFA_DISK_DRIVE_TABLE_NAME
    }
}

impl Id for SfaDiskDrive {
    fn id(&self) -> i32 {
        self.id
    }
}

impl Label for SfaDiskDrive {
    fn label(&self) -> &str {
        "SFA Disk Drive"
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for SfaDiskDrive {
    fn from(row: Row) -> Self {
        Self {
            id: row.get::<_, i32>("id"),
            index: row.get::<_, i32>("index"),
            failed: row.get("failed"),
            health_state_reason: row.get("health_state_reason"),
            health_state: row
                .get::<_, i16>("health_state")
                .try_into()
                .unwrap_or_default(),
            member_index: row.get::<_, Option<i16>>("member_index"),
            member_state: row
                .get::<_, i16>("member_state")
                .try_into()
                .unwrap_or_default(),
            enclosure_index: row.get::<_, i32>("enclosure_index"),
            slot_number: row.get::<_, i32>("slot_number"),
            storage_system: row.get("storage_system"),
        }
    }
}

pub const SFA_JOB_TABLE_NAME: TableName = TableName("chroma_core_sfajob");

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SfaJob {
    pub id: i32,
    pub index: i32,
    pub sub_target_index: Option<i32>,
    pub sub_target_type: Option<SubTargetType>,
    pub job_type: JobType,
    pub state: JobState,
    pub storage_system: String,
}

impl Name for SfaJob {
    fn table_name() -> TableName<'static> {
        SFA_JOB_TABLE_NAME
    }
}

impl Id for SfaJob {
    fn id(&self) -> i32 {
        self.id
    }
}

impl Label for SfaJob {
    fn label(&self) -> &str {
        "SFA Job"
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for SfaJob {
    fn from(row: Row) -> Self {
        Self {
            id: row.get::<_, i32>("id"),
            index: row.get::<_, i32>("index"),
            sub_target_index: row.get::<_, Option<i32>>("sub_target_index"),
            sub_target_type: row
                .get::<_, Option<i16>>("sub_target_type")
                .map(|x| x.try_into().unwrap_or_default()),
            job_type: row.get::<_, i16>("job_type").try_into().unwrap_or_default(),
            state: row.get::<_, i16>("state").try_into().unwrap_or_default(),
            storage_system: row.get("storage_system"),
        }
    }
}

pub const SFA_POWER_SUPPLY_TABLE_NAME: TableName = TableName("chroma_core_sfapowersupply");

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SfaPowerSupply {
    pub id: i32,
    pub index: i32,
    pub enclosure_index: i32,
    pub health_state: HealthState,
    pub health_state_reason: String,
    pub position: i16,
    pub storage_system: String,
}

impl Name for SfaPowerSupply {
    fn table_name() -> TableName<'static> {
        SFA_POWER_SUPPLY_TABLE_NAME
    }
}

impl Id for SfaPowerSupply {
    fn id(&self) -> i32 {
        self.id
    }
}

impl Label for SfaPowerSupply {
    fn label(&self) -> &str {
        "SFA Power Supply"
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for SfaPowerSupply {
    fn from(row: Row) -> Self {
        Self {
            id: row.get::<_, i32>("id"),
            index: row.get::<_, i32>("index"),
            health_state: row
                .get::<_, i16>("health_state")
                .try_into()
                .unwrap_or_default(),
            health_state_reason: row.get("health_state_reason"),
            enclosure_index: row.get::<_, i32>("enclosure_index"),
            position: row.get::<_, i16>("position"),
            storage_system: row.get("storage_system"),
        }
    }
}

pub const SFA_CONTROLLER_TABLE_NAME: TableName = TableName("chroma_core_sfacontroller");

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SfaController {
    pub id: i32,
    pub index: i32,
    pub enclosure_index: i32,
    pub health_state: HealthState,
    pub health_state_reason: String,
    pub child_health_state: HealthState,
    pub storage_system: String,
}

impl Name for SfaController {
    fn table_name() -> TableName<'static> {
        SFA_CONTROLLER_TABLE_NAME
    }
}

impl Id for SfaController {
    fn id(&self) -> i32 {
        self.id
    }
}

impl Label for SfaController {
    fn label(&self) -> &str {
        "SFA Controller"
    }
}

#[cfg(feature = "postgres-interop")]
impl From<Row> for SfaController {
    fn from(row: Row) -> Self {
        Self {
            id: row.get::<_, i32>("id"),
            index: row.get::<_, i32>("index"),
            enclosure_index: row.get::<_, i32>("enclosure_index"),
            health_state: row
                .get::<_, i16>("health_state")
                .try_into()
                .unwrap_or_default(),
            health_state_reason: row.get("health_state_reason"),
            child_health_state: row
                .get::<_, i16>("child_health_state")
                .try_into()
                .unwrap_or_default(),
            storage_system: row.get("storage_system"),
        }
    }
}
