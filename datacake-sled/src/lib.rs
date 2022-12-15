//! `datacake-sled` provides an implementation of the Storage trait backed by the sled embedded key-value database.
//! For every single keyspace two trees are created, one with the keyspace prefixed by `__metadata_` and the other
//! prefixed by `__data_`, with the former containing all document metadata, and the latter containing the actual
//! documents.
//!
//! Design wise due to the lack of documentation around the requirements for implementing the `Storage` trait, I attempted
//! to combine the `MemStore` and `SqliteStore` semantics, however there is a bit of funkiness when it comes to handling
//! items marked as tombstones but not yet deleted, see the corresponding notes in the storage trait definition comments

pub mod utils;

use std::sync::Arc;

use anyhow::Result;
use datacake_cluster::{BulkMutationError, Document, Storage};
use datacake_crdt::{HLCTimestamp, Key};
use models::{Metadata, SledDocument};
use sled::transaction::TransactionError;
use sled::IVec;
use utils::*;

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
        let keyspace_bytes = keyspace.as_bytes();
        let meta_tree_key = metadata_keyspace(keyspace_bytes);
        let data_tree_key = data_keyspace(keyspace_bytes);

        let keys = keys.collect::<Vec<_>>();
        log::debug!("removing tombstones {:?}", keys);
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
                if let Ok(Some(_data)) = data_tx.get(doc_key) {
                    let meta_data: Metadata = match meta_tx.get(doc_key) {
                        Ok(Some(value)) => value.into(),
                        _ => return,
                    };
                    if !meta_data.tombstoned {
                        return;
                    }
                    data_db_batch.remove(&doc_key);
                    meta_db_batch.remove(&doc_key);
                } else {
                    if let Ok(Some(_)) = meta_tx.get(doc_key) {
                        meta_db_batch.remove(&doc_key);
                    }
                }
            });
            data_tx.apply_batch(&data_db_batch)?;
            meta_tx.apply_batch(&meta_db_batch)?;
            Ok::<(), ConflictableTransactionError<()>>(())
        }) {
            log::error!("remove_tombstones(keyspace={}) failed {:#?}", keyspace, err);
            match err {
                TransactionError::Abort(_) => {
                    return Err(BulkMutationError::empty_with_error(
                        sled::Error::Unsupported("transaction aborted".to_string()),
                    ));
                },
                _ => {
                    return Err(BulkMutationError::new(
                        sled::Error::Unsupported(format!(
                            "an unexpected error happened {:#?}",
                            err
                        )),
                        vec![],
                    ))
                },
            }
        }
        Ok(())
    }

    async fn put(&self, keyspace: &str, doc: Document) -> Result<(), Self::Error> {
        if let Err(err) = self.multi_put(keyspace, [doc].into_iter()).await {
            return Err(err.into_inner());
        }
        Ok(())
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        use sled::transaction::ConflictableTransactionError;
        use sled::Transactional;
        let keyspace_bytes = keyspace.as_bytes();
        let meta_keyspace = metadata_keyspace(keyspace_bytes);
        let data_keyspace = data_keyspace(keyspace_bytes);

        let meta_tree = self
            .db
            .open_tree(meta_keyspace)
            .map_err(BulkMutationError::empty_with_error)?;
        let data_tree = self
            .db
            .open_tree(data_keyspace)
            .map_err(BulkMutationError::empty_with_error)?;
        let documents = documents.collect::<Vec<_>>();
        log::debug!(
            "putting documents {:?}",
            documents.iter().map(|doc| doc.id).collect::<Vec<_>>()
        );
        if let Err(err) = (&meta_tree, &data_tree).transaction(|(meta_tx, data_tx)| {
            let mut meta_db_batch = sled::Batch::default();
            let mut data_db_batch = sled::Batch::default();

            documents.iter().for_each(|doc| {
                let sled_doc = SledDocument::new(doc.clone());
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
            log::error!("multi_put(keyspace={}) failed {:#?}", keyspace, err);
            match err {
                TransactionError::Abort(_) => {
                    return Err(BulkMutationError::empty_with_error(
                        sled::Error::Unsupported("transaction aborted".to_string()),
                    ));
                },
                _ => {
                    return Err(BulkMutationError::new(
                        sled::Error::Unsupported(format!(
                            "an unexpected error happened {:#?}",
                            err
                        )),
                        vec![],
                    ))
                },
            }
        };
        Ok(())
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error> {
        if let Err(err) = self
            .mark_many_as_tombstone(keyspace, [(doc_id, timestamp)].into_iter())
            .await
        {
            return Err(err.into_inner());
        }
        Ok(())
    }

    /// NOTE: the expected behavior seems to be that if the data record doesnt exist and this is called
    /// then create the metadata tombstone record anyways
    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = (Key, HLCTimestamp)> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let keyspace_bytes = keyspace.as_bytes();
        let meta_keyspace = metadata_keyspace(keyspace_bytes);
        let data_keyspace = data_keyspace(keyspace_bytes);
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
        log::debug!(
            "putting documents {:?}",
            documents.iter().map(|(id, _)| id).collect::<Vec<_>>()
        );
        if let Err(err) = (&meta_tree, &data_tree).transaction(|(meta_tx, data_tx)| {
            let mut meta_batch = sled::Batch::default();
            let mut data_batch = sled::Batch::default();

            for (id, ts) in documents.iter() {
                let doc_key = id.to_be_bytes();
                match data_tx.get(doc_key) {
                    Ok(Some(value)) => {
                        let mut sled_doc: SledDocument = value.clone().into();
                        sled_doc.set_as_tombstone();
                        match meta_tx.get(doc_key) {
                            Ok(Some(value)) => {
                                let mut meta_doc: Metadata = value.into();
                                meta_doc.tombstoned = true;
                                meta_doc.last_updated = *ts;
                                meta_batch.insert(&doc_key, meta_doc);
                                data_batch.remove(&doc_key);
                            },
                            _ => continue,
                        }
                    },
                    _ => {
                        // persist the metadata record
                        // TODO: this is actually unclear on whether or not
                        //       we should be doing this. the comments of the
                        //       various traits say this should not be inserted
                        //       as the key doesnt exist and is a noop.
                        //
                        //       the test suite however will fail unless the
                        //       metadata is present so this suggests something is wrong
                        meta_batch.insert(
                            &doc_key,
                            Metadata {
                                id: *id,
                                last_updated: *ts,
                                tombstoned: true,
                            },
                        );
                    },
                }
            }
            meta_tx.apply_batch(&meta_batch)?;
            data_tx.apply_batch(&data_batch)?;
            Ok::<(), ConflictableTransactionError<()>>(())
        }) {
            log::error!(
                "mark_many_as_tombstone(keyspace={}) failed {:#?}",
                keyspace,
                err
            );
            match err {
                TransactionError::Abort(_) => {
                    return Err(BulkMutationError::empty_with_error(
                        sled::Error::Unsupported("transaction aborted".to_string()),
                    ));
                },
                _ => {
                    return Err(BulkMutationError::new(
                        sled::Error::Unsupported(format!(
                            "an unexpected error happened {:#?}",
                            err
                        )),
                        vec![],
                    ))
                },
            }
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
                    return Ok(Some(docs[0].clone()));
                }
            },
            Err(err) => return Err(err),
        }
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> Result<Self::DocsIter, Self::Error> {
        use sled::transaction::ConflictableTransactionError;
        use sled::Transactional;
        let keyspace_bytes = keyspace.as_bytes();
        let data_keyspace = data_keyspace(keyspace_bytes);
        let meta_keyspace = metadata_keyspace(keyspace_bytes);
        let data_db_tree = self.db.open_tree(data_keyspace)?;
        let meta_db_tree = self.db.open_tree(meta_keyspace)?;
        let doc_ids = doc_ids.collect::<Vec<_>>();

        match (&meta_db_tree, &data_db_tree).transaction(|(meta_tx, data_tx)| {
            let mut buffer: Vec<Document> = Vec::with_capacity(doc_ids.len());
            doc_ids.iter().for_each(|doc_id| {
                let doc_key = doc_id.to_be_bytes();
                match meta_tx.get(doc_key) {
                    Ok(Some(_value)) => match data_tx.get(doc_key) {
                        Ok(Some(value)) => {
                            let sled_document: SledDocument = value.into();
                            let sled_doc: Document = sled_document.into();
                            buffer.push(sled_doc);
                        },
                        Ok(None) => (),
                        Err(err) => log::error!(
                            "failed to fetch document_id(keyspace={}, id={}): {:#?}",
                            keyspace,
                            doc_id,
                            err
                        ),
                    },
                    Ok(None) => (),
                    Err(err) => {
                        log::error!(
                            "failed to fetch metadata_id(keyspace={}, id={}): {:#?}",
                            keyspace,
                            doc_id,
                            err
                        )
                    },
                }
            });
            Ok::<Vec<Document>, ConflictableTransactionError<()>>(buffer)
        }) {
            Ok(docs) => Ok(Box::new(docs.into_iter())),
            Err(err) => {
                log::error!("multi_get(keyspace={}) failed {:#?}", keyspace, err);
                return Err(sled::Error::Unsupported("failed to get docs".to_string()));
            },
        }
    }
}

mod models {
    use datacake_cluster::Document;
    use datacake_crdt::{HLCTimestamp, Key};
    use sled::IVec;

    pub struct SledKey(pub Key);
    /// the size in bytes that the datastore adds to the document
    /// which is not used by the rest of datacake
    pub const SLED_DOCUMENT_METADATA_SIZE: usize = 1;

    impl From<&IVec> for SledKey {
        fn from(s: &IVec) -> Self {
            let mut u_bytes: [u8; 8] = [0_u8; 8];
            u_bytes.copy_from_slice(&s[0..8]);
            SledKey(u64::from_be_bytes(u_bytes))
        }
    }

    /// simple mirror of the `Document` type but allowing
    /// for convenient conversion to and rom IVec
    ///
    /// TODO: store the actuald document as an `innner` field
    #[derive(Clone)]
    pub struct SledDocument {
        pub inner: Document,
        pub tombstoned: bool,
    }

    impl SledDocument {
        pub fn set_as_tombstone(&mut self) {
            self.inner.data.clear();
            self.tombstoned = true;
        }
        pub fn new(doc: Document) -> Self {
            Self {
                inner: doc,
                tombstoned: false,
            }
        }
    }

    #[derive(Clone)]
    pub struct Metadata {
        pub id: Key,

        /// The timestamp of when the document was last updated.
        pub last_updated: HLCTimestamp,

        pub tombstoned: bool,
    }
    impl From<SledDocument> for Document {
        fn from(sd: SledDocument) -> Self {
            Self {
                id: sd.inner.id,
                last_updated: sd.inner.last_updated,
                data: sd.inner.data,
            }
        }
    }

    impl From<SledDocument> for sled::IVec {
        fn from(sd: SledDocument) -> Self {
            let mut buffer = sd.inner.pack();
            if sd.tombstoned {
                buffer.push(1);
            } else {
                buffer.push(0);
            }
            Self::from(buffer)
        }
    }

    impl From<sled::IVec> for SledDocument {
        fn from(v: sled::IVec) -> Self {
            // we need to provide all but the last remaining bytes
            // this is because the sled datastore adds additional metadata
            // to the document which is not used by the rest of datacake
            // therefore the buffer needs to strip the remaining contents
            let doc = Document::unpack(&v[0..v.len() - SLED_DOCUMENT_METADATA_SIZE]);
            Self {
                inner: doc,
                tombstoned: v[v.len() - SLED_DOCUMENT_METADATA_SIZE] == 1,
            }
        }
    }

    impl From<Metadata> for sled::IVec {
        fn from(sd: Metadata) -> Self {
            let mut buffer = Vec::with_capacity(std::mem::size_of_val(&sd));
            buffer.extend_from_slice(&sd.id.to_le_bytes()[..]);
            buffer.extend_from_slice(&sd.last_updated.pack()[..]);
            if sd.tombstoned {
                buffer.push(1)
            } else {
                buffer.push(0);
            }
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
                tombstoned: buffer[8 + 14] == 1,
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
