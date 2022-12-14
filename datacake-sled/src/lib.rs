use std::collections::{HashMap, HashSet};
use std::sync::Arc;
pub mod error;
use anyhow::Result;
use datacake_cluster::{BulkMutationError, Document, Storage};
use datacake_crdt::{HLCTimestamp, Key};
use models::{SledDocument, SledKey};
use sled::IVec;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone)]
pub struct SledStorage {
    /// Our main db, this stores all of our documents directly.
    db: Arc<sled::Db>,
    /// a pending list of doc ids that have been marked as tombstoned
    /// but aren't yet deleted
    tombstoned_keys: Arc<RwLock<HashMap<String, HashSet<Key>>>>,
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
        Ok(Self {
            db: Arc::new(db),
            tombstoned_keys: Arc::new(RwLock::new(HashMap::with_capacity(128))),
        })
    }
    pub(crate) fn __add_tombstone(
        &self,
        cache_lock: &mut RwLockWriteGuard<'_, HashMap<String, HashSet<u64>>>,
        keyspace: &str,
        keys: &[Key],
    ) {
        if let Some(cache_lock) = cache_lock.get_mut(keyspace) {
            keys.iter().for_each(|key| {
                cache_lock.insert(*key);
            });
        } else {
            let mut hash_set = HashSet::with_capacity(keys.len());
            keys.iter().for_each(|key| {
                hash_set.insert(*key);
            });
            cache_lock.insert(keyspace.to_string(), hash_set);
        }
    }
    pub(crate) fn __remove_tombstone(
        &self,
        cache_lock: &mut RwLockWriteGuard<'_, HashMap<String, HashSet<u64>>>,
        keyspace: &str,
        keys: &[Key],
    ) {
        if let Some(cache_lock) = cache_lock.get_mut(keyspace) {
            keys.iter().for_each(|key| {
                cache_lock.remove(key);
            });
        } else {
        }
    }
    pub(crate) fn __is_tombstoned(
        &self,
        cache_lock: &RwLockReadGuard<'_, HashMap<String, HashSet<u64>>>,
        keyspace: &str,
        key: Key,
    ) -> bool {
        if let Some(keys) = cache_lock.get(keyspace) {
            return keys.contains(&key);
        }
        false
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
                if let Ok(tn) = String::from_utf8(tn.to_vec()) {
                    Some(tn)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        Ok(tree_names)
    }

    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> Result<Self::MetadataIter, Self::Error> {
        let tree = self.db.open_tree(keyspace.as_bytes())?;
        let cache_lock = self.tombstoned_keys.read().await;
        let list = tree
            .into_iter()
            .flatten()
            .map(|(key, value)| {
                let sled_key: SledKey = (&key).into();
                let is_tombstoned =
                    self.__is_tombstoned(&cache_lock, keyspace, sled_key.0);
                let doc: SledDocument = value.into();
                (doc.id, doc.last_updated, is_tombstoned)
            })
            .collect::<Vec<_>>()
            .into_iter();
        Ok(Box::new(list))
    }

    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let mut db_batch = sled::Batch::default();
        let tree = self
            .db
            .open_tree(keyspace.as_bytes())
            .map_err(BulkMutationError::empty_with_error)?;
        let mut cache_lock = self.tombstoned_keys.write().await;
        let keys: Vec<Key> = keys
            .map(|key| {
                db_batch.remove(&key.to_be_bytes());
                key
            })
            .collect::<Vec<_>>();
        self.__remove_tombstone(&mut cache_lock, keyspace, &keys[..]);
        tree.apply_batch(db_batch)
            .map_err(BulkMutationError::empty_with_error)?;
        drop(cache_lock);
        tree.flush_async()
            .await
            .map_err(BulkMutationError::empty_with_error)?;
        Ok(())
    }

    async fn put(&self, keyspace: &str, doc: Document) -> Result<(), Self::Error> {
        let doc_id = doc.id;
        let sled_doc: SledDocument = doc.into();
        let doc_bytes = sled_doc.id.to_be_bytes();
        let tree = self.db.open_tree(keyspace.as_bytes())?;
        let mut cache_lock = self.tombstoned_keys.write().await;
        self.__remove_tombstone(&mut cache_lock, keyspace, &[doc_id]);
        tree.insert(doc_bytes, sled_doc)?;
        tree.flush_async().await?;
        Ok(())
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let mut db_batch = sled::Batch::default();
        let tree = self
            .db
            .open_tree(keyspace.as_bytes())
            .map_err(BulkMutationError::empty_with_error)?;
        let mut cache_lock = self.tombstoned_keys.write().await;
        let keys: Vec<Key> = documents
            .into_iter()
            .map(|doc| {
                let doc_id = doc.id;
                let sled_doc: SledDocument = doc.into();
                let doc_bytes = sled_doc.id.to_be_bytes();
                db_batch.insert(&doc_bytes, sled_doc);
                doc_id
            })
            .collect();
        self.__remove_tombstone(&mut cache_lock, keyspace, &keys[..]);
        tree.apply_batch(db_batch)
            .map_err(BulkMutationError::empty_with_error)?;
        drop(cache_lock);
        tree.flush_async()
            .await
            .map_err(BulkMutationError::empty_with_error)?;
        Ok(())
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error> {
        let tree = self.db.open_tree(keyspace.as_bytes())?;
        let doc_bytes = doc_id.to_be_bytes();
        if let Ok(Some(value)) = tree.get(doc_bytes) {
            let mut sled_doc: SledDocument = value.into();
            sled_doc.mark_as_tombstone(timestamp);
            let mut cache_lock = self.tombstoned_keys.write().await;
            self.__add_tombstone(&mut cache_lock, keyspace, &[doc_id]);
            tree.insert(doc_bytes, sled_doc)?;
            drop(cache_lock);
            tree.flush_async().await?;
        }
        Ok(())
    }

    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = (Key, HLCTimestamp)> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let mut db_batch = sled::Batch::default();
        let tree = self
            .db
            .open_tree(keyspace.as_bytes())
            .map_err(BulkMutationError::empty_with_error)?;
        let mut cache_lock = self.tombstoned_keys.write().await;
        let doc_ids = documents.into_iter().filter_map(|(doc_id, ts)| {
            let doc_bytes = doc_id.to_be_bytes();
            let mut sled_doc: SledDocument = match tree.get(doc_bytes) {
                Ok(Some(d)) => d.into(),
                Ok(None) => {
                    // for some reason it looks like datacake expects cases like this
                    // to actually insert the object, but with empty data.
                    //
                    // todo: report this
                    log::warn!("found no records for doc_id {}",doc_id);
                    let doc = Document::new(doc_id, ts, vec![]);
                    doc.into()
                },
                Err(err) => {
                    log::warn!("found failed to find records for doc_id {}, keyspace {}, {:#?}",doc_id, keyspace, err);
                    // todo: log
                    return None;
                },
            };
            sled_doc.mark_as_tombstone(ts);
            db_batch.insert(&doc_bytes, sled_doc);
            Some(doc_id)
        }).collect::<Vec<Key>>();

        self.__add_tombstone(&mut cache_lock, keyspace, &doc_ids[..]);

        tree.apply_batch(db_batch)
            .map_err(BulkMutationError::empty_with_error)?;
        // clean the lock
        drop(cache_lock);

        tree.flush_async()
            .await
            .map_err(BulkMutationError::empty_with_error)?;
        Ok(())
    }

    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<Document>, Self::Error> {
        let cache_lock = self.tombstoned_keys.read().await;
        if self.__is_tombstoned(&cache_lock, keyspace, doc_id) {
            return Ok(None);
        }
        drop(cache_lock);

        let tree = self.db.open_tree(keyspace.as_bytes())?;
        let doc_id = doc_id.to_be_bytes();
        match tree.get(doc_id) {
            Ok(Some(d)) => {
                let sled_doc: SledDocument = d.into();
                Ok(Some(sled_doc.into()))
            },
            Ok(None) => Ok(None),
            Err(err) => {
                log::warn!(
                    "found failed to find records for doc_id {}, keyspace {}, {:#?}",
                    u64::from_be_bytes(doc_id),
                    keyspace,
                    err
                );
                // todo: log
                return Err(sled::Error::CollectionNotFound(IVec::from(&doc_id)));
            },
        }
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> Result<Self::DocsIter, Self::Error> {
        let tree = self.db.open_tree(keyspace.as_bytes())?;
        let cache_lock = self.tombstoned_keys.read().await;
        let keyspace_lock = cache_lock.get(keyspace);
        Ok(Box::new(
            doc_ids
                .into_iter()
                .filter_map(|doc_id| {
                    if let Some(keyspace_lock) = keyspace_lock {
                        if keyspace_lock.contains(&doc_id) {
                            return None;
                        }
                    }
                    let doc_id = doc_id.to_be_bytes();
                    match tree.get(doc_id) {
                        Ok(Some(doc)) => {
                            let sled_doc: SledDocument = doc.into();
                            let doc: Document = sled_doc.into();
                            Some(doc)
                        },
                        _ => {
                            log::error!(
                                "failed to lookup doc_id {}, keyspace {}",
                                u64::from_be_bytes(doc_id),
                                keyspace
                            );

                            None // todo: handle
                        },
                    }
                })
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
    async fn default_keyspace(&self) -> Option<String> {
        const DEFAULT_SPACE: &str = "__sled__default";
        Some(String::from(DEFAULT_SPACE))
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

    impl SledDocument {
        pub fn mark_as_tombstone(&mut self, ts: HLCTimestamp) {
            self.data.clear();
            self.last_updated = ts;
            assert!(self.data.is_empty());
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
