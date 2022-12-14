use std::collections::{HashMap, HashSet};
use std::sync::Arc;
pub mod error;
use anyhow::{Result, anyhow};
use datacake_cluster::{BulkMutationError, Document, Storage};
use datacake_crdt::{get_unix_timestamp_ms, HLCTimestamp, Key};
use models::{Metadata, SledDocument, SledKey};
use sled::IVec;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

const METADATA_KEYSPACE_PREFIX: &[u8] = b"__metadata_";
const DATA_KEYSPACE_PREFIX: &[u8] = b"_data_";

/// given a keyspace, format the name for its metadata keyspace
pub fn metadata_keyspace(keyspace: &[u8]) -> Vec<u8> {
    const META_LEN: usize = METADATA_KEYSPACE_PREFIX.len();
    let mut buf = Vec::with_capacity(META_LEN + keyspace.len());
    buf.extend_from_slice(METADATA_KEYSPACE_PREFIX);
    buf.extend_from_slice(keyspace);
    buf
}

/// given a keyspace, format the name for its data keyspace
pub fn data_keyspace(keyspace: &[u8]) -> Vec<u8> {
    const DATA_LEN: usize = DATA_KEYSPACE_PREFIX.len();
    let mut buf = Vec::with_capacity(DATA_LEN + keyspace.len());
    buf.extend_from_slice(DATA_KEYSPACE_PREFIX);
    buf.extend_from_slice(keyspace);
    buf
}

#[derive(Clone)]
pub struct SledStorage {
    /// Our main db, this stores all of our documents directly.
    db: Arc<sled::Db>,
}

impl SledStorage {
    pub fn open_temporary() -> anyhow::Result<Self> {
        Self::open(
            sled::Config::default()
                .temporary(true)
                .print_profile_on_drop(true),
        )
    }
    pub fn open(conf: sled::Config) -> anyhow::Result<Self> {
        let db = conf.open()?;
        Ok(Self { db: Arc::new(db) })
    }
}

#[async_trait::async_trait]
impl Storage for SledStorage {
    type Error = sled::Error;
    type DocsIter = Box<dyn Iterator<Item = Document>>;
    type MetadataIter = Box<dyn Iterator<Item = (Key, HLCTimestamp, bool)>>;

    async fn get_keyspace_list(&self) -> Result<Vec<String>, Self::Error> {
        let tree_names = self
            .db
            .tree_names()
            .iter()
            .filter_map(|tn| {
                if let Some(meta_data_keyspace) = tn.strip_prefix(b"__metadata_") {
                    if let Ok(tn) = String::from_utf8(meta_data_keyspace.to_vec()) {
                        return Some(tn);
                    }
                }
                None
            })
            .collect::<Vec<_>>();
        Ok(tree_names)
    }

    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> Result<Self::MetadataIter, Self::Error> {
        let tree_key = metadata_keyspace(keyspace.as_bytes());
        let tree = self.db.open_tree(tree_key)?;
        let list = tree.into_iter().flatten().map(|(_, value)| {
            let doc: Metadata = value.into();
            (doc.id, doc.last_updated, doc.tombstoned)
        });
        Ok(Box::new(list))
    }

    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        use sled::transaction::ConflictableTransactionError;
        use sled::Transactional;
        let meta_tree_key = metadata_keyspace(keyspace.as_bytes());
        let data_tree_key = data_keyspace(keyspace.as_bytes());

        let keys = keys.collect::<Vec<_>>();
        let meta_tree = self
            .db
            .open_tree(meta_tree_key)
            .map_err(BulkMutationError::empty_with_error)?;
        let data_tree = self
            .db
            .open_tree(data_tree_key)
            .map_err(BulkMutationError::empty_with_error)?;
        if let Err(err) = (&meta_tree, &data_tree).transaction(|(meta_tx, data_tx)| {
            let mut data_db_batch = sled::Batch::default();
            let mut meta_db_batch = sled::Batch::default();
            keys.iter().for_each(|key| {
                let doc_key = key.to_be_bytes();
                match data_tx.get(&doc_key) {
                    Ok(Some(data)) => {
                        let mut sled_document: SledDocument = data.into();
                        sled_document.set_as_tombstone();
                        let sled_ivec: IVec = sled_document.into();
                        data_db_batch.insert(&doc_key, sled_ivec);
                        meta_db_batch.remove(&doc_key);
                    },
                    _ => return,
                }
            });
            data_tx.apply_batch(&data_db_batch)?;
            meta_tx.apply_batch(&meta_db_batch)?;
            Ok::<(), ConflictableTransactionError<()>>(())
        }) {
            return Err(BulkMutationError::new(
                sled::Error::Unsupported(format!("{:#?}", err)),
                vec![],
            ));
        }

        Ok(())
    }

    async fn put(&self, keyspace: &str, doc: Document) -> Result<(), Self::Error> {
        use sled::transaction::ConflictableTransactionError;
        use sled::Transactional;
        let doc_bytes = doc.id.to_be_bytes();

        let meta_keyspace = metadata_keyspace(keyspace.as_bytes());
        let data_keyspace = data_keyspace(keyspace.as_bytes());

        let meta_tree = self.db.open_tree(meta_keyspace)?;
        let data_tree = self.db.open_tree(data_keyspace)?;
        if let Err(err) = (&meta_tree, &data_tree).transaction(|(meta_tx, data_tx)| {
            let sled_doc: SledDocument = doc.clone().into();
            let sled_data: IVec = sled_doc.into();
            data_tx.insert(&doc_bytes, sled_data)?;
            meta_tx.insert(
                &doc_bytes,
                Metadata {
                    id: doc.id,
                    last_updated: doc.last_updated,
                    tombstoned: false,
                },
            )?;
            Ok::<(), ConflictableTransactionError<()>>(())
        }) {
            log::error!("transaction encountered error {:#?}", err);
        };
        Ok(())
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        use sled::transaction::ConflictableTransactionError;
        use sled::Transactional;
        let meta_keyspace = metadata_keyspace(keyspace.as_bytes());
        let data_keyspace = data_keyspace(keyspace.as_bytes());

        let meta_tree = self
            .db
            .open_tree(meta_keyspace)
            .map_err(BulkMutationError::empty_with_error)?;
        let data_tree = self
            .db
            .open_tree(data_keyspace)
            .map_err(BulkMutationError::empty_with_error)?;
        let documents = documents.collect::<Vec<_>>();
        if let Err(err) = (&meta_tree, &data_tree).transaction(|(meta_tx, data_tx)| {
            let mut meta_db_batch = sled::Batch::default();
            let mut data_db_batch = sled::Batch::default();

            documents.iter().for_each(|doc| {
                let sled_doc: SledDocument = doc.clone().into();
                let sled_data: IVec = sled_doc.into();
                data_db_batch.insert(&doc.id.to_be_bytes(), sled_data);
                meta_db_batch.insert(
                    &doc.id.to_be_bytes(),
                    Metadata {
                        id: doc.id,
                        last_updated: doc.last_updated,
                        tombstoned: false,
                    },
                );
            });
            meta_tx.apply_batch(&meta_db_batch)?;
            data_tx.apply_batch(&data_db_batch)?;
            Ok::<(), ConflictableTransactionError<()>>(())
        }) {
            log::error!("transaction encountered error {:#?}", err);
        };

        Ok(())
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error> {
        let meta_keyspace = metadata_keyspace(keyspace.as_bytes());
        let data_keyspace = data_keyspace(keyspace.as_bytes());

        let doc_key = doc_id.to_be_bytes();

        use sled::transaction::ConflictableTransactionError;
        use sled::Transactional;
        let meta_tree = self.db.open_tree(meta_keyspace)?;
        let data_tree = self.db.open_tree(data_keyspace)?;
        if let Err(err) = (&meta_tree, &data_tree).transaction(|(meta_tx, data_tx)| {
            match data_tx.get(&doc_key) {
                Ok(Some((value))) => {
                    let mut sled_doc: SledDocument = value.clone().into();
                    sled_doc.set_as_tombstone();
                    meta_tx.insert(
                        &doc_key,
                        Metadata {
                            id: sled_doc.id,
                            last_updated: timestamp,
                            tombstoned: true,
                        },
                    )?;
                    data_tx.insert(&doc_key, sled_doc)?;
                },
                _ => (),
            }

            //data_tx.remove(&doc_key)?;
            Ok::<(), ConflictableTransactionError<()>>(())
        }) {
            log::error!("transaction encountered error {:#?}", err);
        };
        Ok(())
    }

    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = (Key, HLCTimestamp)> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let meta_keyspace = metadata_keyspace(keyspace.as_bytes());
        let data_keyspace = data_keyspace(keyspace.as_bytes());
        use sled::transaction::ConflictableTransactionError;
        use sled::Transactional;
        let meta_tree = self
            .db
            .open_tree(meta_keyspace)
            .map_err(BulkMutationError::empty_with_error)?;
        let data_tree = self
            .db
            .open_tree(data_keyspace)
            .map_err(BulkMutationError::empty_with_error)?;

        let documents = documents.collect::<Vec<_>>();
        if let Err(err) = (&meta_tree, &data_tree).transaction(|(meta_tx, data_tx)| {
            let mut meta_batch = sled::Batch::default();
            let mut data_batch = sled::Batch::default();

            for (id, ts) in documents.iter() {
                let doc_key = id.to_be_bytes();
                match data_tx.get(&doc_key) {
                    Ok(Some((value))) => {
                        let mut sled_doc: SledDocument = value.clone().into();
                        sled_doc.set_as_tombstone();
                        meta_batch.insert(
                            &doc_key,
                            Metadata {
                                id: sled_doc.id,
                                last_updated: *ts,
                                tombstoned: true,
                            },
                        );
                        data_batch.insert(&doc_key, sled_doc);
                    },
                    _ => (),
                }
            }
            meta_tx.apply_batch(&meta_batch)?;
            data_tx.apply_batch(&data_batch)?;
            Ok::<(), ConflictableTransactionError<()>>(())
        }) {
            log::error!("transaction encountered error {:#?}", err);
        };

        Ok(())
    }

    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<Document>, Self::Error> {
        match self.multi_get(keyspace, [doc_id].into_iter()).await {
            Ok(docs_iter) => {
                let docs = docs_iter.collect::<Vec<_>>();
                if docs.is_empty() {
                    return Ok(None);
                } else {
                    return Ok(Some(docs[0].clone()))
                }
            }
            Err(err) => return Err(sled::Error::Unsupported(format!("failed to get docs {:#?}", err))),

        }
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> Result<Self::DocsIter, Self::Error> {
        use sled::transaction::ConflictableTransactionError;
        use sled::Transactional;
        let data_keyspace = data_keyspace(keyspace.as_bytes());
        let meta_keyspace = metadata_keyspace(keyspace.as_bytes());
        let data_db_tree = self.db.open_tree(data_keyspace)?;
        let meta_db_tree = self.db.open_tree(meta_keyspace)?;
        let doc_ids = doc_ids.collect::<Vec<_>>();

        match (&meta_db_tree, &data_db_tree).transaction(|(meta_tx, data_tx)| {
                let mut buffer: Vec<Document> = Vec::with_capacity(doc_ids.len());
                doc_ids.iter().for_each(|doc_id| {
                    let doc_key = doc_id.to_be_bytes();
                    match meta_tx.get(&doc_key) {
                        Ok(Some(value)) => {
                            let meta_data: Metadata = value.into();   
                            if !meta_data.tombstoned {
                                match data_tx.get(&doc_key) {
                                    Ok(Some(value)) => {
                                        let sled_document: SledDocument = value.into();
                                        let sled_doc: Document = sled_document.into();
                                        buffer.push(sled_doc);
                                    },
                                    _ => ()
                                }
                            }

                        }
                        _ => (),
                    }
                });
                Ok::<Vec<Document>, ConflictableTransactionError<()>>(buffer)
            }) {
                Ok(docs) => Ok(Box::new(docs.into_iter())),
                Err(err) => return Err(sled::Error::Unsupported(format!("failed to get docs {:#?}", err))),
            }
    }
    async fn default_keyspace(&self) -> Option<String> {
        None
    }
}

mod models {
    use datacake_cluster::Document;
    use datacake_crdt::{HLCTimestamp, Key};
    use sled::IVec;

    pub struct SledKey(pub Key);

    impl From<&IVec> for SledKey {
        fn from(s: &IVec) -> Self {
            let mut u_bytes: [u8; 8] = [0_u8; 8];
            u_bytes.copy_from_slice(&s[0..8]);
            SledKey(u64::from_be_bytes(u_bytes))
        }
    }

    /// simple mirror of the `Document` type but allowing
    /// for convenient conversion to and rom IVec
    #[derive(Clone)]
    pub struct SledDocument {
        /// The unique id of the document.
        pub id: Key,

        /// The timestamp of when the document was last updated.
        pub last_updated: HLCTimestamp,

        /// The raw binary data of the document's value.
        pub data: bytes::Bytes,
    }

    impl SledDocument {
        pub fn set_as_tombstone(&mut self) {
            self.data.clear();
        }
    }

    #[derive(Clone)]
    pub struct Metadata {
        pub id: Key,

        /// The timestamp of when the document was last updated.
        pub last_updated: HLCTimestamp,

        pub tombstoned: bool,
    }

    impl From<Document> for SledDocument {
        fn from(d: Document) -> Self {
            Self {
                id: d.id,
                last_updated: d.last_updated,
                data: d.data,
            }
        }
    }

    impl From<SledDocument> for Document {
        fn from(sd: SledDocument) -> Self {
            Self {
                id: sd.id,
                last_updated: sd.last_updated,
                data: sd.data,
            }
        }
    }

    impl From<SledDocument> for sled::IVec {
        fn from(sd: SledDocument) -> Self {
            let mut buffer = Vec::with_capacity(std::mem::size_of_val(&sd));
            buffer.extend_from_slice(&sd.id.to_le_bytes()[..]);
            buffer.extend_from_slice(&sd.last_updated.pack()[..]);
            buffer.extend_from_slice(&sd.data);
            Self::from(buffer)
        }
    }

    impl From<sled::IVec> for SledDocument {
        fn from(v: sled::IVec) -> Self {
            let doc = Document::unpack(&v);
            doc.into()
        }
    }

    impl From<Metadata> for sled::IVec {
        fn from(sd: Metadata) -> Self {
            let mut buffer = Vec::with_capacity(std::mem::size_of_val(&sd));
            buffer.extend_from_slice(&sd.id.to_le_bytes()[..]);
            buffer.extend_from_slice(&sd.last_updated.pack()[..]);
            buffer.push(sd.tombstoned as u8);
            Self::from(buffer)
        }
    }

    impl From<sled::IVec> for Metadata {
        fn from(buffer: sled::IVec) -> Self {
            let mut id: [u8; 8] = [0_u8; 8];
            id.copy_from_slice(&buffer[0..8]);
            let ts = HLCTimestamp::unpack(&buffer[8..8 + 14]).unwrap();
            Self {
                id: u64::from_le_bytes(id),
                last_updated: ts,
                tombstoned: buffer[8 + 14] != 0,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datacake_cluster::test_suite;

    //use datacake_cluster::test_suite;
    use crate::SledStorage;
    #[tokio::test]
    async fn test_sled_key_logic() {}
    #[tokio::test]
    async fn test_storage_logic() {
        std::env::set_var("RUST_LOG", "debug");
        let _ = tracing_subscriber::fmt::try_init();
        let storage = SledStorage::open_temporary().unwrap();
        test_suite::run_test_suite(storage, true).await;
    }
}
