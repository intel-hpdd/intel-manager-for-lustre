use iml_wire_types::warp_drive::Cache;

pub(crate) fn get_cache() -> Cache {
    static DATA: &[u8] = include_bytes!("./fixture.json");

    serde_json::from_slice(&DATA).unwrap()
}
