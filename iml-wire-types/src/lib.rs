// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

pub mod client;
pub mod db;
pub mod graphql_duration;
pub mod high_availability;
pub mod sfa;
pub mod snapshot;
pub mod stratagem;
pub mod task;
pub mod warp_drive;

use chrono::{DateTime, Utc};
use db::LogMessageRecord;
use ipnetwork::{Ipv4Network, Ipv6Network};
use std::{
    cmp::{Ord, Ordering},
    collections::{BTreeMap, BTreeSet, HashMap},
    convert::TryFrom,
    convert::TryInto,
    fmt, io,
    num::ParseIntError,
    ops::Deref,
    sync::Arc,
};

#[derive(Eq, PartialEq, Hash, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct PluginName(pub String);

impl From<PluginName> for String {
    fn from(PluginName(s): PluginName) -> Self {
        s
    }
}

impl From<&str> for PluginName {
    fn from(name: &str) -> Self {
        Self(name.into())
    }
}

impl fmt::Display for PluginName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(
    Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Clone, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct Fqdn(pub String);

impl From<Fqdn> for String {
    fn from(Fqdn(s): Fqdn) -> Self {
        s
    }
}

impl From<&str> for Fqdn {
    fn from(s: &str) -> Self {
        Fqdn(s.into())
    }
}

impl From<&String> for Fqdn {
    fn from(s: &String) -> Self {
        Fqdn(s.as_str().into())
    }
}

impl From<String> for Fqdn {
    fn from(s: String) -> Self {
        Fqdn(s)
    }
}

impl fmt::Display for Fqdn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Id(pub String);

impl From<&str> for Id {
    fn from(name: &str) -> Self {
        Self(name.into())
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The payload from the agent.
/// One or many can be packed into an `Envelope`
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Message {
    Data {
        fqdn: Fqdn,
        plugin: PluginName,
        session_id: Id,
        session_seq: u64,
        body: serde_json::Value,
    },
    SessionCreateRequest {
        fqdn: Fqdn,
        plugin: PluginName,
    },
}

/// `Envelope` of `Messages` sent to the manager.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Envelope {
    pub messages: Vec<Message>,
    pub server_boot_time: String,
    pub client_start_time: String,
}

impl Envelope {
    pub fn new(
        messages: Vec<Message>,
        client_start_time: impl Into<String>,
        server_boot_time: impl Into<String>,
    ) -> Self {
        Self {
            messages,
            server_boot_time: server_boot_time.into(),
            client_start_time: client_start_time.into(),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ManagerMessage {
    SessionCreateResponse {
        fqdn: Fqdn,
        plugin: PluginName,
        session_id: Id,
    },
    Data {
        fqdn: Fqdn,
        plugin: PluginName,
        session_id: Id,
        body: serde_json::Value,
    },
    SessionTerminate {
        fqdn: Fqdn,
        plugin: PluginName,
        session_id: Id,
    },
    SessionTerminateAll {
        fqdn: Fqdn,
    },
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct ManagerMessages {
    pub messages: Vec<ManagerMessage>,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PluginMessage {
    SessionTerminate {
        fqdn: Fqdn,
        plugin: PluginName,
        session_id: Id,
    },
    SessionCreate {
        fqdn: Fqdn,
        plugin: PluginName,
        session_id: Id,
    },
    Data {
        fqdn: Fqdn,
        plugin: PluginName,
        session_id: Id,
        session_seq: u64,
        body: serde_json::Value,
    },
}

pub trait FlatQuery {
    fn query() -> Vec<(&'static str, &'static str)> {
        vec![("limit", "0")]
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, serde::Deserialize, serde::Serialize)]
pub struct ActionName(pub String);

impl From<&str> for ActionName {
    fn from(name: &str) -> Self {
        Self(name.into())
    }
}

impl From<&String> for ActionName {
    fn from(name: &String) -> Self {
        Self(name.as_str().into())
    }
}

impl Deref for ActionName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

impl From<String> for ActionName {
    fn from(name: String) -> Self {
        Self(name)
    }
}

impl fmt::Display for ActionName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct ActionId(pub String);

impl fmt::Display for ActionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Things we can do with actions
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Action {
    ActionStart {
        action: ActionName,
        args: serde_json::value::Value,
        id: ActionId,
    },
    ActionCancel {
        id: ActionId,
    },
}

impl Action {
    pub fn get_id(&self) -> &ActionId {
        match self {
            Self::ActionStart { id, .. } | Self::ActionCancel { id, .. } => id,
        }
    }
}

impl TryFrom<Action> for serde_json::Value {
    type Error = serde_json::Error;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        serde_json::to_value(action)
    }
}

/// Actions can be run either locally or remotely.
/// Besides the node these are run on, the interface should
/// be the same.
///
/// This should probably be collapsed into a single struct over an enum at some point.
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ActionType {
    Remote((Fqdn, Action)),
    Local(Action),
}

/// The result of running the action on an agent.
#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct ActionResult {
    pub id: ActionId,
    pub result: Result<serde_json::value::Value, String>,
}

pub type AgentResult = std::result::Result<serde_json::Value, String>;

pub trait ToJsonValue {
    fn to_json_value(&self) -> Result<serde_json::Value, String>;
}

impl<T: serde::Serialize> ToJsonValue for T {
    fn to_json_value(&self) -> Result<serde_json::Value, String> {
        serde_json::to_value(self).map_err(|e| format!("{:?}", e))
    }
}

pub trait ToBytes {
    fn to_bytes(&self) -> Result<Vec<u8>, serde_json::error::Error>;
}

impl<T: serde::Serialize> ToBytes for T {
    fn to_bytes(&self) -> Result<Vec<u8>, serde_json::error::Error> {
        serde_json::to_vec(&self)
    }
}

/// Arguments to all Task Actions on agents: FSNAME, Task Args, FID LIST
#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct TaskAction(pub String, pub HashMap<String, String>, pub Vec<FidItem>);

#[derive(
    serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash,
)]
#[serde(try_from = "String")]
#[serde(into = "String")]
pub struct CompositeId(pub i32, pub i32);

impl fmt::Display for CompositeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}

impl From<CompositeId> for String {
    fn from(x: CompositeId) -> Self {
        format!("{}", x)
    }
}

impl TryFrom<String> for CompositeId {
    type Error = Box<dyn std::error::Error>;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let xs: Vec<_> = s.split(':').collect();

        if xs.len() != 2 {
            return Err("Could not convert to CompositeId, String did not contain 2 parts.".into());
        }

        let x = xs[0].parse::<i32>()?;
        let y = xs[1].parse::<i32>()?;

        Ok(Self(x, y))
    }
}

pub trait ToCompositeId {
    fn composite_id(&self) -> CompositeId;
}

impl<T: ToCompositeId> ToCompositeId for &Arc<T> {
    fn composite_id(&self) -> CompositeId {
        let t: &T = self.deref();

        t.composite_id()
    }
}

impl<T: ToCompositeId> ToCompositeId for Arc<T> {
    fn composite_id(&self) -> CompositeId {
        let t: &T = self.deref();

        t.composite_id()
    }
}

pub trait Label {
    fn label(&self) -> &str;
}

pub trait EndpointName {
    fn endpoint_name() -> &'static str;
}

pub trait EndpointNameSelf {
    fn endpoint_name(&self) -> &'static str;
}

impl<T: EndpointName> EndpointNameSelf for T {
    fn endpoint_name(&self) -> &'static str {
        Self::endpoint_name()
    }
}

/// The type of lock
#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Eq, PartialEq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum LockType {
    Read,
    Write,
}

/// The Action associated with a `LockChange`
#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Eq, PartialEq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum LockAction {
    Add,
    Remove,
}

/// A change to be applied to `Locks`
#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Eq, PartialEq, Hash)]
pub struct LockChange {
    pub uuid: String,
    pub job_id: u64,
    pub content_type_id: i32,
    pub item_id: i32,
    pub description: String,
    pub lock_type: LockType,
    pub action: LockAction,
}

impl ToCompositeId for LockChange {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id, self.item_id)
    }
}

/// Meta is the metadata object returned by a fetch call
#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct Meta {
    pub limit: u32,
    pub next: Option<String>,
    pub offset: u32,
    pub previous: Option<String>,
    pub total_count: u32,
}

/// ApiList contains the metadata and the `Vec` of objects returned by a fetch call
#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct ApiList<T> {
    pub meta: Meta,
    pub objects: Vec<T>,
}

impl<T> ApiList<T> {
    pub fn new(objects: Vec<T>) -> Self {
        Self {
            meta: Meta {
                limit: 0,
                next: None,
                offset: 0,
                previous: None,
                total_count: objects.len() as u32,
            },
            objects,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Conf {
    pub allow_anonymous_read: bool,
    pub build: String,
    pub version: String,
    pub exa_version: Option<String>,
    pub is_release: bool,
    pub branding: Branding,
    pub use_stratagem: bool,
    pub use_snapshots: bool,
    pub monitor_sfa: bool,
}

impl Default for Conf {
    fn default() -> Self {
        Self {
            allow_anonymous_read: true,
            build: "Not Loaded".into(),
            version: "0".into(),
            exa_version: None,
            is_release: false,
            branding: Branding::default(),
            use_stratagem: false,
            use_snapshots: false,
            monitor_sfa: false,
        }
    }
}

impl EndpointName for Conf {
    fn endpoint_name() -> &'static str {
        "conf"
    }
}

// An available action from `/api/action/`
#[derive(serde::Deserialize, serde::Serialize, PartialEq, Clone, Debug)]
pub struct AvailableAction {
    pub args: Option<HashMap<String, Option<u64>>>,
    pub composite_id: CompositeId,
    pub class_name: Option<String>,
    pub confirmation: Option<String>,
    pub display_group: u64,
    pub display_order: u64,
    pub long_description: String,
    pub state: Option<String>,
    pub verb: String,
}

impl EndpointName for AvailableAction {
    fn endpoint_name() -> &'static str {
        "action"
    }
}

/// A `NtpConfiguration` record from `/api/ntp_configuration/`
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct NtpConfiguration {
    pub content_type_id: i32,
    pub id: i32,
    pub immutable_state: bool,
    pub label: String,
    pub not_deleted: Option<bool>,
    pub resource_uri: String,
    pub state: String,
    pub state_modified_at: String,
}

impl EndpointName for NtpConfiguration {
    fn endpoint_name() -> &'static str {
        "ntp_configuration"
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct ClientMount {
    pub filesystem_name: String,
    pub mountpoints: Vec<String>,
    pub state: String,
}

/// A Host record from `/api/host/`
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct Host {
    pub address: String,
    pub boot_time: Option<String>,
    pub client_mounts: Option<Vec<ClientMount>>,
    pub content_type_id: i32,
    pub corosync_configuration: Option<String>,
    pub corosync_ring0: String,
    pub fqdn: String,
    pub id: i32,
    pub immutable_state: bool,
    pub install_method: String,
    pub label: String,
    pub lnet_configuration: String,
    pub member_of_active_filesystem: bool,
    pub needs_update: bool,
    pub nids: Option<Vec<String>>,
    pub nodename: String,
    pub pacemaker_configuration: Option<String>,
    pub private_key: Option<String>,
    pub private_key_passphrase: Option<String>,
    pub resource_uri: String,
    pub root_pw: Option<String>,
    pub server_profile: ServerProfile,
    pub state: String,
    pub state_modified_at: String,
}

impl Host {
    /// Get associated LNet configuration id
    pub fn lnet_id(&self) -> Option<i32> {
        let id = iml_api_utils::extract_id(&self.lnet_configuration)?;

        id.parse::<i32>().ok()
    }
    /// Get associated Corosync configuration id
    pub fn corosync_id(&self) -> Option<i32> {
        let id = iml_api_utils::extract_id(self.corosync_configuration.as_ref()?)?;

        id.parse::<i32>().ok()
    }
    /// Get associated Pacemaker configuration id
    pub fn pacemaker_id(&self) -> Option<i32> {
        let id = iml_api_utils::extract_id(self.pacemaker_configuration.as_ref()?)?;

        id.parse::<i32>().ok()
    }
}

impl FlatQuery for Host {}

impl ToCompositeId for Host {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id, self.id)
    }
}

impl ToCompositeId for &Host {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id, self.id)
    }
}

impl Label for Host {
    fn label(&self) -> &str {
        &self.label
    }
}

impl Label for &Host {
    fn label(&self) -> &str {
        &self.label
    }
}

impl db::Id for Host {
    fn id(&self) -> i32 {
        self.id
    }
}

impl db::Id for &Host {
    fn id(&self) -> i32 {
        self.id
    }
}

impl EndpointName for Host {
    fn endpoint_name() -> &'static str {
        "host"
    }
}

/// A server profile record from api/server_profile/
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct ServerProfile {
    pub corosync: bool,
    pub corosync2: bool,
    pub default: bool,
    pub initial_state: String,
    pub managed: bool,
    pub name: String,
    pub ntp: bool,
    pub pacemaker: bool,
    pub repolist: Vec<String>,
    pub resource_uri: String,
    pub ui_description: String,
    pub ui_name: String,
    pub user_selectable: bool,
    pub worker: bool,
}

impl FlatQuery for ServerProfile {}

impl EndpointName for ServerProfile {
    fn endpoint_name() -> &'static str {
        "server_profile"
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProfileTest {
    pub description: String,
    pub error: String,
    pub pass: bool,
    pub test: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CmdWrapper {
    pub command: Command,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[cfg_attr(feature = "graphql", derive(juniper::GraphQLObject))]
pub struct Command {
    pub cancelled: bool,
    pub complete: bool,
    pub created_at: String,
    pub errored: bool,
    pub id: i32,
    pub jobs: Vec<String>,
    pub logs: String,
    pub message: String,
    pub resource_uri: String,
}

impl EndpointName for Command {
    fn endpoint_name() -> &'static str {
        "command"
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JobLock {
    pub locked_item_content_type_id: i32,
    pub locked_item_id: i32,
    pub locked_item_uri: String,
    pub resource_uri: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AvailableTransition {
    pub label: String,
    pub state: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Job<T> {
    pub available_transitions: Vec<AvailableTransition>,
    pub cancelled: bool,
    pub class_name: String,
    pub commands: Vec<String>,
    pub created_at: String,
    pub description: String,
    pub errored: bool,
    pub id: i32,
    pub modified_at: String,
    pub read_locks: Vec<JobLock>,
    pub resource_uri: String,
    pub state: String,
    pub step_results: HashMap<String, T>,
    pub steps: Vec<String>,
    pub wait_for: Vec<String>,
    pub write_locks: Vec<JobLock>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Check {
    pub name: String,
    pub value: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HostValididity {
    pub address: String,
    pub status: Vec<Check>,
    pub valid: bool,
    pub profiles: HashMap<String, Vec<ProfileTest>>,
}

pub type TestHostJob = Job<HostValididity>;

impl<T> EndpointName for Job<T> {
    fn endpoint_name() -> &'static str {
        "job"
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Step {
    pub args: HashMap<String, serde_json::value::Value>,
    pub backtrace: String,
    pub class_name: String,
    pub console: String,
    pub created_at: String,
    pub description: String,
    pub id: i32,
    pub log: String,
    pub modified_at: String,
    pub resource_uri: String,
    pub result: Option<String>,
    pub state: String,
    pub step_count: i32,
    pub step_index: i32,
}

impl EndpointName for Step {
    fn endpoint_name() -> &'static str {
        "step"
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct FilesystemConfParams {
    #[serde(rename = "llite.max_cached_mb")]
    pub llite_max_cached_mb: Option<String>,
    #[serde(rename = "llite.max_read_ahead_mb")]
    pub llite_max_read_ahead_mb: Option<String>,
    #[serde(rename = "llite.max_read_ahead_whole_mb")]
    pub llite_max_read_ahead_whole_mb: Option<String>,
    #[serde(rename = "llite.statahead_max")]
    pub llite_statahead_max: Option<String>,
    #[serde(rename = "sys.at_early_margin")]
    pub sys_at_early_margin: Option<String>,
    #[serde(rename = "sys.at_extra")]
    pub sys_at_extra: Option<String>,
    #[serde(rename = "sys.at_history")]
    pub sys_at_history: Option<String>,
    #[serde(rename = "sys.at_max")]
    pub sys_at_max: Option<String>,
    #[serde(rename = "sys.at_min")]
    pub sys_at_min: Option<String>,
    #[serde(rename = "sys.ldlm_timeout")]
    pub sys_ldlm_timeout: Option<String>,
    #[serde(rename = "sys.timeout")]
    pub sys_timeout: Option<String>,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Clone)]
pub struct MdtConfParams {
    #[serde(rename = "lov.qos_prio_free")]
    lov_qos_prio_free: Option<String>,
    #[serde(rename = "lov.qos_threshold_rr")]
    lov_qos_threshold_rr: Option<String>,
    #[serde(rename = "lov.stripecount")]
    lov_stripecount: Option<String>,
    #[serde(rename = "lov.stripesize")]
    lov_stripesize: Option<String>,
    #[serde(rename = "mdt.MDS.mds.threads_max")]
    mdt_mds_mds_threads_max: Option<String>,
    #[serde(rename = "mdt.MDS.mds.threads_min")]
    mdt_mds_mds_threads_min: Option<String>,
    #[serde(rename = "mdt.MDS.mds_readpage.threads_max")]
    mdt_mds_mds_readpage_threads_max: Option<String>,
    #[serde(rename = "mdt.MDS.mds_readpage.threads_min")]
    mdt_mds_mds_readpage_threads_min: Option<String>,
    #[serde(rename = "mdt.MDS.mds_setattr.threads_max")]
    mdt_mds_mds_setattr_threads_max: Option<String>,
    #[serde(rename = "mdt.MDS.mds_setattr.threads_min")]
    mdt_mds_mds_setattr_threads_min: Option<String>,
    #[serde(rename = "mdt.hsm_control")]
    mdt_hsm_control: Option<String>,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Clone)]
pub struct OstConfParams {
    #[serde(rename = "osc.active")]
    osc_active: Option<String>,
    #[serde(rename = "osc.max_dirty_mb")]
    osc_max_dirty_mb: Option<String>,
    #[serde(rename = "osc.max_pages_per_rpc")]
    osc_max_pages_per_rpc: Option<String>,
    #[serde(rename = "osc.max_rpcs_in_flight")]
    osc_max_rpcs_in_flight: Option<String>,
    #[serde(rename = "ost.OSS.ost.threads_max")]
    ost_oss_ost_threads_max: Option<String>,
    #[serde(rename = "ost.OSS.ost.threads_min")]
    ost_oss_ost_threads_min: Option<String>,
    #[serde(rename = "ost.OSS.ost_create.threads_max")]
    ost_oss_ost_create_threads_max: Option<String>,
    #[serde(rename = "ost.OSS.ost_create.threads_min")]
    ost_oss_ost_create_threads_min: Option<String>,
    #[serde(rename = "ost.OSS.ost_io.threads_max")]
    ost_oss_ost_io_threads_max: Option<String>,
    #[serde(rename = "ost.OSS.ost_io.threads_min")]
    ost_oss_ost_io_threads_min: Option<String>,
    #[serde(rename = "ost.read_cache_enable")]
    ost_read_cache_enable: Option<String>,
    #[serde(rename = "ost.readcache_max_filesize")]
    ost_readcache_max_filesize: Option<String>,
    #[serde(rename = "ost.sync_journal")]
    ost_sync_journal: Option<String>,
    #[serde(rename = "ost.sync_on_lock_cancel")]
    ost_sync_on_lock_cancel: Option<String>,
    #[serde(rename = "ost.writethrough_cache_enable")]
    ost_writethrough_cache_enable: Option<String>,
}

/// A Volume record from api/volume/
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct Volume {
    pub filesystem_type: Option<String>,
    pub id: i32,
    pub kind: String,
    pub label: String,
    pub resource_uri: String,
    pub size: Option<i64>,
    pub status: Option<String>,
    pub storage_resource: Option<String>,
    pub usable_for_lustre: bool,
    pub volume_nodes: Vec<VolumeNode>,
}

impl FlatQuery for Volume {}

impl Label for Volume {
    fn label(&self) -> &str {
        &self.label
    }
}

impl Label for &Volume {
    fn label(&self) -> &str {
        &self.label
    }
}

impl db::Id for Volume {
    fn id(&self) -> i32 {
        self.id
    }
}

impl db::Id for &Volume {
    fn id(&self) -> i32 {
        self.id
    }
}

impl EndpointName for Volume {
    fn endpoint_name() -> &'static str {
        "volume"
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct VolumeNode {
    pub host: String,
    pub host_id: i32,
    pub host_label: String,
    pub id: i32,
    pub path: String,
    pub primary: bool,
    pub resource_uri: String,
    #[serde(rename = "use")]
    pub _use: bool,
    pub volume_id: i32,
}

impl FlatQuery for VolumeNode {}

impl EndpointName for VolumeNode {
    fn endpoint_name() -> &'static str {
        "volume_node"
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
#[serde(untagged)]
pub enum TargetConfParam {
    MdtConfParam(MdtConfParams),
    OstConfParam(OstConfParams),
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
#[serde(untagged)]
pub enum VolumeOrResourceUri {
    ResourceUri(String),
    Volume(Volume),
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum TargetKind {
    Mgt,
    Mdt,
    Ost,
}

/// A Target record from /api/target/
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct Target<T> {
    pub active_host: Option<String>,
    pub active_host_name: String,
    pub conf_params: Option<T>,
    pub content_type_id: i32,
    pub failover_server_name: String,
    pub failover_servers: Vec<String>,
    pub filesystem: Option<String>,
    pub filesystem_id: Option<i32>,
    pub filesystem_name: Option<String>,
    pub filesystems: Option<Vec<FilesystemShort>>,
    pub ha_label: Option<String>,
    pub id: i32,
    pub immutable_state: bool,
    pub index: Option<i32>,
    pub inode_count: Option<u64>,
    pub inode_size: Option<i32>,
    pub kind: TargetKind,
    pub label: String,
    pub name: String,
    pub primary_server: String,
    pub primary_server_name: String,
    pub resource_uri: String,
    pub state: String,
    pub state_modified_at: String,
    pub uuid: Option<String>,
    pub volume: VolumeOrResourceUri,
    pub volume_name: String,
}

impl<T> FlatQuery for Target<T> {
    fn query() -> Vec<(&'static str, &'static str)> {
        vec![("limit", "0"), ("dehydrate__volume", "false")]
    }
}

impl<T> ToCompositeId for Target<T> {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id, self.id)
    }
}

impl<T> ToCompositeId for &Target<T> {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id, self.id)
    }
}

impl<T> Label for Target<T> {
    fn label(&self) -> &str {
        &self.label
    }
}

impl<T> Label for &Target<T> {
    fn label(&self) -> &str {
        &self.label
    }
}

impl<T> EndpointName for Target<T> {
    fn endpoint_name() -> &'static str {
        "target"
    }
}

impl<T> db::Id for Target<T> {
    fn id(&self) -> i32 {
        self.id
    }
}

pub type Mdt = Target<MdtConfParams>;
pub type Mgt = Target<Option<TargetConfParam>>;
pub type Ost = Target<OstConfParams>;

#[derive(serde::Deserialize, serde::Serialize, PartialEq, Clone, Debug)]
pub struct HsmControlParamMdt {
    pub id: String,
    pub kind: String,
    pub resource: String,
    pub conf_params: MdtConfParams,
}

/// HsmControlParams used for hsm actions
#[derive(serde::Deserialize, serde::Serialize, PartialEq, Clone, Debug)]
pub struct HsmControlParam {
    pub long_description: String,
    pub param_key: String,
    pub param_value: String,
    pub verb: String,
    pub mdt: HsmControlParamMdt,
}

/// A Filesystem record from /api/filesystem/
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct Filesystem {
    pub bytes_free: Option<f64>,
    pub bytes_total: Option<f64>,
    pub cdt_mdt: Option<String>,
    pub cdt_status: Option<String>,
    pub client_count: Option<u64>,
    pub conf_params: FilesystemConfParams,
    pub content_type_id: i32,
    pub files_free: Option<u64>,
    pub files_total: Option<u64>,
    pub hsm_control_params: Option<Vec<HsmControlParam>>,
    pub id: i32,
    pub immutable_state: bool,
    pub label: String,
    pub mdts: Vec<Mdt>,
    pub mgt: String,
    pub mount_command: String,
    pub mount_path: String,
    pub name: String,
    pub osts: Vec<String>,
    pub resource_uri: String,
    pub state: String,
    pub state_modified_at: String,
}

impl FlatQuery for Filesystem {
    fn query() -> Vec<(&'static str, &'static str)> {
        vec![("limit", "0"), ("dehydrate__mgt", "false")]
    }
}

impl ToCompositeId for Filesystem {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id, self.id)
    }
}

impl ToCompositeId for &Filesystem {
    fn composite_id(&self) -> CompositeId {
        CompositeId(self.content_type_id, self.id)
    }
}

impl Label for Filesystem {
    fn label(&self) -> &str {
        &self.label
    }
}

impl Label for &Filesystem {
    fn label(&self) -> &str {
        &self.label
    }
}

impl db::Id for Filesystem {
    fn id(&self) -> i32 {
        self.id
    }
}

impl db::Id for &Filesystem {
    fn id(&self) -> i32 {
        self.id
    }
}

impl EndpointName for Filesystem {
    fn endpoint_name() -> &'static str {
        "filesystem"
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct FilesystemShort {
    pub id: i32,
    pub name: String,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Copy, Debug)]
pub enum AlertRecordType {
    AlertState,
    LearnEvent,
    AlertEvent,
    SyslogEvent,
    ClientConnectEvent,
    CommandRunningAlert,
    CommandSuccessfulAlert,
    CommandCancelledAlert,
    CommandErroredAlert,
    CorosyncUnknownPeersAlert,
    CorosyncToManyPeersAlert,
    CorosyncNoPeersAlert,
    CorosyncStoppedAlert,
    StonithNotEnabledAlert,
    PacemakerStoppedAlert,
    HostContactAlert,
    HostOfflineAlert,
    HostRebootEvent,
    UpdatesAvailableAlert,
    TargetOfflineAlert,
    TargetFailoverAlert,
    TargetRecoveryAlert,
    StorageResourceOffline,
    StorageResourceAlert,
    StorageResourceLearnEvent,
    PowerControlDeviceUnavailableAlert,
    IpmiBmcUnavailableAlert,
    LNetOfflineAlert,
    LNetNidsChangedAlert,
    StratagemUnconfiguredAlert,
    TimeOutOfSyncAlert,
    NoTimeSyncAlert,
    MultipleTimeSyncAlert,
    UnknownTimeSyncAlert,
}

impl ToString for AlertRecordType {
    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap().replace("\"", "")
    }
}

#[derive(
    serde::Serialize, serde::Deserialize, Copy, Clone, Debug, PartialOrd, Ord, PartialEq, Eq,
)]
pub enum AlertSeverity {
    DEBUG,
    INFO,
    WARNING,
    ERROR,
    CRITICAL,
}

impl From<AlertSeverity> for i32 {
    fn from(x: AlertSeverity) -> Self {
        match x {
            AlertSeverity::DEBUG => 10,
            AlertSeverity::INFO => 20,
            AlertSeverity::WARNING => 30,
            AlertSeverity::ERROR => 40,
            AlertSeverity::CRITICAL => 50,
        }
    }
}

/// An Alert record from /api/alert/
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct Alert {
    pub _message: Option<String>,
    pub active: Option<bool>,
    pub affected: Option<Vec<String>>,
    pub affected_composite_ids: Option<Vec<CompositeId>>,
    pub alert_item: String,
    pub alert_item_id: Option<i32>,
    pub alert_item_str: String,
    pub alert_type: String,
    pub begin: String,
    pub dismissed: bool,
    pub end: Option<String>,
    pub id: i32,
    pub lustre_pid: Option<i32>,
    pub message: String,
    pub record_type: AlertRecordType,
    pub resource_uri: String,
    pub severity: AlertSeverity,
    pub variant: String,
}

impl FlatQuery for Alert {
    fn query() -> Vec<(&'static str, &'static str)> {
        vec![("limit", "0"), ("active", "true")]
    }
}

impl EndpointName for Alert {
    fn endpoint_name() -> &'static str {
        "alert"
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Substitution {
    pub start: String,
    pub end: String,
    pub label: String,
    pub resource_uri: String,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "postgres-interop", derive(sqlx::Type))]
#[cfg_attr(feature = "graphql", derive(juniper::GraphQLEnum))]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[repr(i16)]
pub enum MessageClass {
    Normal = 0,
    Lustre = 1,
    LustreError = 2,
    Copytool = 3,
    CopytoolError = 4,
}

impl TryFrom<i16> for MessageClass {
    type Error = &'static str;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MessageClass::Normal),
            1 => Ok(MessageClass::Lustre),
            2 => Ok(MessageClass::LustreError),
            3 => Ok(MessageClass::Copytool),
            4 => Ok(MessageClass::CopytoolError),
            _ => Err("Invalid variant for MessageClass"),
        }
    }
}

/// Severities from syslog protocol
///
/// | Code | Severity                                 |
/// |------|------------------------------------------|
/// | 0    | Emergency: system is unusable            |
/// | 1    | Alert: action must be taken immediately  |
/// | 2    | Critical: critical conditions            |
/// | 3    | Error: error conditions                  |
/// | 4    | Warning: warning conditions              |
/// | 5    | Notice: normal but significant condition |
/// | 6    | Informational: informational messages    |
/// | 7    | Debug: debug-level messages              |
///
#[derive(
    serde::Deserialize, serde::Serialize, PartialEq, Eq, Ord, PartialOrd, Clone, Copy, Debug,
)]
#[cfg_attr(feature = "postgres-interop", derive(sqlx::Type))]
#[cfg_attr(feature = "graphql", derive(juniper::GraphQLEnum))]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[repr(i16)]
pub enum LogSeverity {
    Emergency = 0,
    Alert = 1,
    Critical = 2,
    Error = 3,
    Warning = 4,
    Notice = 5,
    Informational = 6,
    Debug = 7,
}

impl TryFrom<i16> for LogSeverity {
    type Error = &'static str;

    fn try_from(value: i16) -> Result<Self, <Self as TryFrom<i16>>::Error> {
        match value {
            0 => Ok(LogSeverity::Emergency),
            1 => Ok(LogSeverity::Alert),
            2 => Ok(LogSeverity::Critical),
            3 => Ok(LogSeverity::Error),
            4 => Ok(LogSeverity::Warning),
            5 => Ok(LogSeverity::Notice),
            6 => Ok(LogSeverity::Informational),
            7 => Ok(LogSeverity::Debug),
            _ => Err("Invalid variant for LogSeverity"),
        }
    }
}

/// An Log record from /api/log/
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Log {
    pub datetime: String,
    pub facility: i32,
    pub fqdn: String,
    pub id: i32,
    pub message: String,
    pub message_class: MessageClass,
    pub resource_uri: String,
    pub severity: LogSeverity,
    pub substitutions: Vec<Substitution>,
    pub tag: String,
}

impl EndpointName for Log {
    fn endpoint_name() -> &'static str {
        "log"
    }
}

// A log message from GraphQL endpoint
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[cfg_attr(feature = "graphql", derive(juniper::GraphQLObject))]
pub struct LogMessage {
    pub id: i32,
    pub datetime: chrono::DateTime<Utc>,
    pub facility: i32,
    pub fqdn: String,
    pub message: String,
    pub message_class: MessageClass,
    pub severity: LogSeverity,
    pub tag: String,
}

impl TryFrom<LogMessageRecord> for LogMessage {
    type Error = &'static str;

    fn try_from(record: LogMessageRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            id: record.id,
            datetime: record.datetime,
            facility: record.facility as i32,
            fqdn: record.fqdn,
            message: record.message,
            message_class: record.message_class.try_into()?,
            severity: record.severity.try_into()?,
            tag: record.tag,
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[cfg_attr(feature = "graphql", derive(juniper::GraphQLEnum))]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SortDir {
    Asc,
    Desc,
}

impl Default for SortDir {
    fn default() -> Self {
        Self::Asc
    }
}

impl Deref for SortDir {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }
}

pub mod logs {
    use crate::LogMessage;

    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
    #[cfg_attr(feature = "graphql", derive(juniper::GraphQLObject))]
    pub struct Meta {
        pub total_count: i32,
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
    #[cfg_attr(feature = "graphql", derive(juniper::GraphQLObject))]
    pub struct LogResponse {
        pub data: Vec<LogMessage>,
        pub meta: Meta,
    }
}

/// A `StratagemConfiguration` record from `api/stratagem_configuration`.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct StratagemConfiguration {
    pub content_type_id: i32,
    pub filesystem: String,
    pub id: i32,
    pub immutable_state: bool,
    pub interval: u64,
    pub label: String,
    pub not_deleted: Option<bool>,
    pub purge_duration: Option<u64>,
    pub report_duration: Option<u64>,
    pub resource_uri: String,
    pub state: String,
    pub state_modified_at: String,
}

impl EndpointName for StratagemConfiguration {
    fn endpoint_name() -> &'static str {
        "stratagem_configuration"
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[cfg_attr(feature = "graphql", derive(juniper::GraphQLObject))]
/// Information about a stratagem report
pub struct StratagemReport {
    /// The filename of the stratagem report
    pub filename: String,
    /// When the report was last modified
    pub modify_time: DateTime<Utc>,
    /// The size of the report in bytes
    pub size: i32,
}

/// An `AlertType` record from `api/alert_type`.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct AlertType {
    pub description: String,
    pub id: String,
    pub resource_uri: String,
}

impl EndpointName for AlertType {
    fn endpoint_name() -> &'static str {
        "alert_type"
    }
}

/// An `AlertSubscription` record from `api/alert_subscription`.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct AlertSubscription {
    pub alert_type: AlertType,
    pub id: i32,
    pub resource_uri: String,
    pub user: String,
}

impl EndpointName for AlertSubscription {
    fn endpoint_name() -> &'static str {
        "alert_subscription"
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum GroupType {
    Superusers,
    FilesystemAdministrators,
    FilesystemUsers,
}

/// A `Group` record from `api/group`.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct Group {
    pub id: i32,
    pub name: GroupType,
    pub resource_uri: String,
}

impl EndpointName for Group {
    fn endpoint_name() -> &'static str {
        "group"
    }
}

/// A `User` record from `api/user`.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct User {
    pub alert_subscriptions: Option<Vec<AlertSubscription>>,
    pub email: String,
    pub first_name: String,
    pub full_name: String,
    pub groups: Option<Vec<Group>>,
    pub id: i32,
    pub is_superuser: bool,
    pub last_name: String,
    pub new_password1: Option<String>,
    pub new_password2: Option<String>,
    pub password1: Option<String>,
    pub password2: Option<String>,
    pub resource_uri: String,
    pub username: String,
}

impl EndpointName for User {
    fn endpoint_name() -> &'static str {
        "user"
    }
}

/// A `Session` record from `api/session`
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct Session {
    pub read_enabled: bool,
    pub resource_uri: String,
    pub user: Option<User>,
}

impl EndpointName for Session {
    fn endpoint_name() -> &'static str {
        "session"
    }
}

pub mod time {
    #[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum Synced {
        Synced,
        Unsynced,
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub enum State {
        None,
        Multiple,
        Synced,
        Unsynced(Option<Offset>),
        Unknown,
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
    pub struct Offset(String);

    impl<T: ToString> From<T> for Offset {
        fn from(s: T) -> Self {
            Self(s.to_string())
        }
    }
}

/// Types used for component checks
#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ElementState {
    pub name: String,
    pub configurable: bool,
}

#[derive(Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum UnitFileState {
    Disabled,
    Enabled,
}

#[derive(Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ActiveState {
    Inactive,
    Active,
    Activating,
    Failed,
}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum RunState {
    Stopped,
    Enabled,
    Activating,
    Failed,
    Started,
    Setup, // Enabled + Started
}

impl Default for RunState {
    fn default() -> Self {
        Self::Stopped
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ServiceState {
    Unconfigured,
    Configured(RunState),
}

impl Default for ServiceState {
    fn default() -> Self {
        Self::Unconfigured
    }
}

impl fmt::Display for ServiceState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Unconfigured => f.pad(&format!("{:?}", self)),
            Self::Configured(r) => f.pad(&format!("{:?}", r)),
        }
    }
}

/// standard:provider:ocftype (e.g. ocf:heartbeat:ZFS, or stonith:fence_ipmilan)
#[derive(serde::Deserialize, serde::Serialize, PartialEq, Clone, Debug)]
pub struct ResourceAgentType {
    // e.g. ocf, lsb, stonith, etc..
    pub standard: String,
    // e.g. heartbeat, lustre, chroma
    pub provider: Option<String>,
    // e.g. Lustre, ZFS
    pub ocftype: String,
}

impl ResourceAgentType {
    pub fn new<'a>(
        standard: impl Into<Option<&'a str>>,
        provider: impl Into<Option<&'a str>>,
        ocftype: impl Into<Option<&'a str>>,
    ) -> Self {
        Self {
            standard: standard.into().map(str::to_string).unwrap_or_default(),
            provider: provider.into().map(str::to_string),
            ocftype: ocftype.into().map(str::to_string).unwrap_or_default(),
        }
    }
}

impl fmt::Display for ResourceAgentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.provider {
            Some(provider) => write!(f, "{}:{}:{}", self.standard, provider, self.ocftype),
            None => write!(f, "{}:{}", self.standard, self.ocftype),
        }
    }
}

/// Information about pacemaker resource agents
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ResourceAgentInfo {
    pub agent: ResourceAgentType,
    pub id: String,
    pub args: HashMap<String, String>,
    pub ops: PacemakerOperations,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderingKind {
    Mandatory,
    Optional,
    Serialize,
}

impl fmt::Display for OrderingKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&format!("{:?}", self))
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PacemakerScore {
    Infinity,
    Value(i32),
    NegInfinity,
}

impl fmt::Display for PacemakerScore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            PacemakerScore::Value(v) => write!(f, "{}", v),
            PacemakerScore::Infinity => write!(f, "INFINITY"),
            PacemakerScore::NegInfinity => write!(f, "-INFINITY"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PacemakerKindOrScore {
    Kind(OrderingKind),
    Score(PacemakerScore),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PacemakerOperations {
    // Time to wait for Resource to start
    pub start: Option<String>,
    // Time of monitor interval
    pub monitor: Option<String>,
    // Time to wait for Resource to stop
    pub stop: Option<String>,
}

impl PacemakerOperations {
    pub fn new(
        start: impl Into<Option<String>>,
        monitor: impl Into<Option<String>>,
        stop: impl Into<Option<String>>,
    ) -> Self {
        Self {
            start: start.into(),
            monitor: monitor.into(),
            stop: stop.into(),
        }
    }
}

/// Information about pacemaker resource agents
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResourceConstraint {
    Ordering {
        id: String,
        first: String,
        then: String,
        // While the documentation only lists Kind the xml schema
        // (constraints-2.9.rng) shows Kind or Score being valid
        kind: Option<PacemakerKindOrScore>,
    },
    Location {
        id: String,
        rsc: String,
        node: String,
        score: PacemakerScore,
    },
    Colocation {
        id: String,
        rsc: String,
        with_rsc: String,
        score: PacemakerScore,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ConfigState {
    Unknown,
    Default,
    // components is in default configuration
    IML,
    // matches what IML would do
    Other,
}

impl Default for ConfigState {
    fn default() -> Self {
        Self::Unknown
    }
}

impl fmt::Display for ConfigState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&format!("{:?}", self))
    }
}

#[derive(Debug, Default, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
pub struct ComponentState<T: Default> {
    pub service: ServiceState,
    pub config: ConfigState,
    pub elements: Vec<ElementState>,
    pub info: String,
    pub state: T,
}

/// An OST Pool record from `/api/ostpool/`
#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OstPoolApi {
    pub id: i32,
    pub resource_uri: String,
    #[serde(flatten)]
    pub ost: OstPool,
}

impl EndpointName for OstPoolApi {
    fn endpoint_name() -> &'static str {
        "ostpool"
    }
}

impl FlatQuery for OstPoolApi {}

impl std::fmt::Display for OstPoolApi {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[#{}] {}", self.id, self.ost)
    }
}

/// Type Sent between ostpool agent daemon and service
/// FS Name -> Set of OstPools
pub type FsPoolMap = BTreeMap<String, BTreeSet<OstPool>>;

#[derive(Debug, Default, Clone, Eq, serde::Serialize, serde::Deserialize)]
pub struct OstPool {
    pub name: String,
    pub filesystem: String,
    pub osts: Vec<String>,
}

impl std::fmt::Display for OstPool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}.{} [{}]",
            self.filesystem,
            self.name,
            self.osts.join(", ")
        )
    }
}

impl Ord for OstPool {
    fn cmp(&self, other: &Self) -> Ordering {
        let x = self.filesystem.cmp(&other.filesystem);
        if x != Ordering::Equal {
            return x;
        }
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for OstPool {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OstPool {
    fn eq(&self, other: &Self) -> bool {
        self.filesystem == other.filesystem && self.name == other.name
    }
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JournalMessage {
    pub datetime: std::time::Duration,
    pub severity: JournalPriority,
    pub facility: i16,
    pub source: String,
    pub message: String,
}

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[repr(i16)]
pub enum JournalPriority {
    Emerg,
    Alert,
    Crit,
    Err,
    Warning,
    Notice,
    Info,
    Debug,
}

impl TryFrom<String> for JournalPriority {
    type Error = io::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let x = s
            .parse::<u8>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        match x {
            0 => Ok(Self::Emerg),
            1 => Ok(Self::Alert),
            2 => Ok(Self::Crit),
            3 => Ok(Self::Err),
            4 => Ok(Self::Warning),
            5 => Ok(Self::Notice),
            6 => Ok(Self::Info),
            7 => Ok(Self::Debug),
            x => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Priority {} not in range", x),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum Branding {
    DDN(DdnBranding),
    Whamcloud,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum DdnBranding {
    AI200,
    AI200X,
    AI400,
    AI400X,
    AI7990X,
    ES14K,
    ES14KX,
    ES18K,
    ES18KX,
    ES200NV,
    ES200NVX,
    ES400NV,
    ES400NVX,
    ES7990,
    ES7990X,
    Exascaler,
}

impl fmt::Display for DdnBranding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AI200 => write!(f, "AI200"),
            Self::AI200X => write!(f, "AI200X"),
            Self::AI400 => write!(f, "AI400"),
            Self::AI400X => write!(f, "AI400X"),
            Self::AI7990X => write!(f, "AI7990X"),
            Self::ES14K => write!(f, "ES14K"),
            Self::ES14KX => write!(f, "ES14KX"),
            Self::ES18K => write!(f, "ES18K"),
            Self::ES18KX => write!(f, "ES18KX"),
            Self::ES200NV => write!(f, "ES200NV"),
            Self::ES200NVX => write!(f, "ES200NVX"),
            Self::ES400NV => write!(f, "ES400NV"),
            Self::ES400NVX => write!(f, "ES400NVX"),
            Self::ES7990 => write!(f, "ES7990"),
            Self::ES7990X => write!(f, "ES7990X"),
            Self::Exascaler => write!(f, "EXA5"),
        }
    }
}

impl Default for Branding {
    fn default() -> Self {
        Self::Whamcloud
    }
}

impl From<String> for Branding {
    fn from(x: String) -> Self {
        match x.to_lowercase().as_str() {
            "ai200" => Self::DDN(DdnBranding::AI200),
            "ai200x" => Self::DDN(DdnBranding::AI200X),
            "ai400" => Self::DDN(DdnBranding::AI400),
            "ai400x" => Self::DDN(DdnBranding::AI400X),
            "ai7990x" => Self::DDN(DdnBranding::AI7990X),
            "es14k" => Self::DDN(DdnBranding::ES14K),
            "es14kx" => Self::DDN(DdnBranding::ES14KX),
            "es18k" => Self::DDN(DdnBranding::ES18K),
            "es18kx" => Self::DDN(DdnBranding::ES18KX),
            "es200nv" => Self::DDN(DdnBranding::ES200NV),
            "es200nvx" => Self::DDN(DdnBranding::ES200NVX),
            "es400nv" => Self::DDN(DdnBranding::ES400NV),
            "es400nvx" => Self::DDN(DdnBranding::ES400NVX),
            "es7990" => Self::DDN(DdnBranding::ES7990),
            "es7990x" => Self::DDN(DdnBranding::ES7990X),
            "exascaler" => Self::DDN(DdnBranding::Exascaler),
            _ => Self::Whamcloud,
        }
    }
}

impl fmt::Display for Branding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DDN(x) => write!(f, "{}", x),
            Self::Whamcloud => write!(f, "whamcloud"),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FidError {
    pub fid: String,
    pub data: serde_json::Value,
    pub errno: i16,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FidItem {
    pub fid: String,
    pub data: serde_json::Value,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
#[cfg_attr(feature = "graphql", derive(juniper::GraphQLObject))]
pub struct HotpoolConfiguration {
    /// The id of the config
    pub id: i32,
    /// The filesystem associated with this target
    pub filesystem: String,
    /// Current Hotpool State
    /// Possible Values: unconfigured, configured, stopped, started, removed
    pub state: String,
    pub state_modified_at: DateTime<Utc>,
    pub ha_label: String,
    /// Version of Hotpool Config
    pub version: Option<i32>,

    /// minimum age before mirror to cold pool
    pub minage: i32,
    pub freehi: Option<i32>,
    pub freelo: Option<i32>,
    /// Hot OstPool
    pub hot_id: i32,
    /// Cold OstPool
    pub cold_id: i32,
    /// Purge Task id
    pub purge_id: Option<i32>,
    /// Resync Task id
    pub resync_id: Option<i32>,
    /// Extend Task id
    pub extend_id: Option<i32>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct LustreClient {
    pub id: i32,
    pub state_modified_at: DateTime<Utc>,
    pub state: String,
    pub immutable_state: bool,
    pub not_deleted: Option<bool>,
    pub content_type_id: Option<i32>,
    pub filesystem: String,
    pub host_id: i32,
    pub mountpoints: Vec<String>,
}

#[derive(Debug, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct LdevEntry {
    pub primary: String,
    pub failover: Option<String>,
    pub label: String,
    pub device: String,
}

impl From<&str> for LdevEntry {
    fn from(x: &str) -> Self {
        let parts: Vec<&str> = x.split(' ').collect();

        Self {
            primary: (*parts
                .get(0)
                .unwrap_or_else(|| panic!("LdevEntry must specify a primary server.")))
            .to_string(),
            failover: parts.get(1).map_or_else(
                || panic!("LdevEntry must specify a failover server or '-'."),
                |x| {
                    if *x == "-" {
                        None
                    } else {
                        Some((*x).to_string())
                    }
                },
            ),
            label: (*parts
                .get(2)
                .unwrap_or_else(|| panic!("LdevEntry must specify a label.")))
            .to_string(),
            device: (*parts
                .get(3)
                .unwrap_or_else(|| panic!("LdevEntry must specify a device.")))
            .to_string(),
        }
    }
}

impl fmt::Display for LdevEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {} {}",
            self.primary,
            self.failover.as_deref().unwrap_or("-"),
            self.label,
            self.device
        )
    }
}

impl Ord for LdevEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.label.cmp(&other.label)
    }
}

impl PartialOrd for LdevEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LdevEntry {
    fn eq(&self, other: &Self) -> bool {
        self.label == other.label
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LndType {
    Tcp,
    O2ib,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LinuxType {
    Ethernet,
    Ether,
    Infiniband,
}

impl fmt::Display for LndType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp => write!(f, "tcp"),
            Self::O2ib => write!(f, "o2ib"),
        }
    }
}

#[derive(Debug)]
pub struct InterfaceError(pub String);

impl fmt::Display for InterfaceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for InterfaceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl TryFrom<Option<String>> for LinuxType {
    type Error = InterfaceError;

    fn try_from(val: Option<String>) -> Result<Self, Self::Error> {
        if let Some(val) = val {
            match val.to_ascii_lowercase().trim() {
                "ethernet" => Ok(LinuxType::Ethernet),
                "ether" => Ok(LinuxType::Ether),
                "infiniband" => Ok(LinuxType::Infiniband),
                _ => Err(InterfaceError("Invalid linux network type. Must be one of 'ethernet', 'ether', or 'infiniband'.".into())),
            }
        } else {
            Err(InterfaceError("Interface type is not set.".into()))
        }
    }
}

impl TryFrom<Option<LinuxType>> for LndType {
    type Error = InterfaceError;

    fn try_from(i_type: Option<LinuxType>) -> Result<Self, Self::Error> {
        if let Some(i_type) = i_type {
            match i_type {
                LinuxType::Ethernet => Ok(LndType::Tcp),
                LinuxType::Ether => Ok(LndType::Tcp),
                LinuxType::Infiniband => Ok(LndType::O2ib),
            }
        } else {
            Err(InterfaceError(
                "The linux type is not set and thus cannot be converted to an LndType.".into(),
            ))
        }
    }
}

pub type StatResult = Result<u64, ParseIntError>;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RxStats {
    pub bytes: u64,
    pub packets: u64,
    pub errs: u64,
    pub drop: u64,
    pub fifo: u64,
    pub frame: u64,
    pub compressed: u64,
    pub multicast: u64,
}

impl
    TryFrom<(
        StatResult,
        StatResult,
        StatResult,
        StatResult,
        StatResult,
        StatResult,
        StatResult,
        StatResult,
    )> for RxStats
{
    type Error = ParseIntError;

    fn try_from(
        (v1, v2, v3, v4, v5, v6, v7, v8): (
            StatResult,
            StatResult,
            StatResult,
            StatResult,
            StatResult,
            StatResult,
            StatResult,
            StatResult,
        ),
    ) -> Result<Self, Self::Error> {
        Ok(RxStats {
            bytes: v1?,
            packets: v2?,
            errs: v3?,
            drop: v4?,
            fifo: v5?,
            frame: v6?,
            compressed: v7?,
            multicast: v8?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TxStats {
    pub bytes: u64,
    pub packets: u64,
    pub errs: u64,
    pub drop: u64,
    pub fifo: u64,
    pub colls: u64,
    pub carrier: u64,
    pub compressed: u64,
}

impl
    TryFrom<(
        StatResult,
        StatResult,
        StatResult,
        StatResult,
        StatResult,
        StatResult,
        StatResult,
        StatResult,
    )> for TxStats
{
    type Error = ParseIntError;

    fn try_from(
        (v1, v2, v3, v4, v5, v6, v7, v8): (
            StatResult,
            StatResult,
            StatResult,
            StatResult,
            StatResult,
            StatResult,
            StatResult,
            StatResult,
        ),
    ) -> Result<Self, Self::Error> {
        Ok(TxStats {
            bytes: v1?,
            packets: v2?,
            errs: v3?,
            drop: v4?,
            fifo: v5?,
            colls: v6?,
            carrier: v7?,
            compressed: v8?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InterfaceStats {
    pub rx: RxStats,
    pub tx: TxStats,
}

impl
    TryFrom<(
        Result<RxStats, ParseIntError>,
        Result<TxStats, ParseIntError>,
    )> for InterfaceStats
{
    type Error = ParseIntError;

    fn try_from(
        (rx, tx): (
            Result<RxStats, ParseIntError>,
            Result<TxStats, ParseIntError>,
        ),
    ) -> Result<Self, Self::Error> {
        Ok(InterfaceStats { rx: rx?, tx: tx? })
    }
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct NetworkInterface {
    pub interface: String,
    pub mac_address: Option<String>,
    pub interface_type: Option<LndType>,
    pub inet4_address: Vec<Ipv4Network>,
    pub inet6_address: Vec<Ipv6Network>,
    pub stats: Option<InterfaceStats>,
    pub is_up: bool,
    pub is_slave: bool,
}
