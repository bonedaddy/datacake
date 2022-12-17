pub const METADATA_KEYSPACE_PREFIX: &[u8] = b"__metadata_";
pub const DATA_KEYSPACE_PREFIX: &[u8] = b"_data_";

/// constructs the tree name used by the keyspace for storing metadata
pub fn metadata_keyspace(keyspace: &[u8]) -> Vec<u8> {
    const META_LEN: usize = METADATA_KEYSPACE_PREFIX.len();
    let mut buf = Vec::with_capacity(META_LEN + keyspace.len());
    buf.extend_from_slice(METADATA_KEYSPACE_PREFIX);
    buf.extend_from_slice(keyspace);
    buf
}

/// constructs the tree name used by the keyspace for storing data
pub fn data_keyspace(keyspace: &[u8]) -> Vec<u8> {
    const DATA_LEN: usize = DATA_KEYSPACE_PREFIX.len();
    let mut buf = Vec::with_capacity(DATA_LEN + keyspace.len());
    buf.extend_from_slice(DATA_KEYSPACE_PREFIX);
    buf.extend_from_slice(keyspace);
    buf
}
