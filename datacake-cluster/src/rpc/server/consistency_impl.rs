use std::borrow::Cow;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::str::FromStr;

use async_trait::async_trait;
use datacake_crdt::HLCTimestamp;
use tonic::{Request, Response, Status};

use crate::core::Document;
use crate::keyspace::{KeyspaceGroup, CONSISTENCY_SOURCE_ID};
use crate::node_identifier::NodeID;
use crate::rpc::datacake_api;
use crate::rpc::datacake_api::consistency_api_server::ConsistencyApi;
use crate::rpc::datacake_api::{
    BatchPayload,
    Context,
    MultiPutPayload,
    MultiRemovePayload,
    PutPayload,
    RemovePayload,
};
use crate::storage::Storage;
use crate::{ProgressTracker, PutContext, RpcNetwork};

pub struct ConsistencyService<S>
where
    S: Storage + Send + Sync + 'static,
{
    group: KeyspaceGroup<S>,
    network: RpcNetwork,
}

impl<S> ConsistencyService<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub fn new(group: KeyspaceGroup<S>, network: RpcNetwork) -> Self {
        Self { group, network }
    }

    fn get_put_ctx(&self, ctx: Option<Context>) -> Result<Option<PutContext>, Status> {
        let ctx = if let Some(info) = ctx {
            let remote_addr = info.node_addr.parse::<SocketAddr>().map_err(|e| {
                Status::internal(format!(
                    "Failed to parse remote node addr {} - {}",
                    info.node_addr, e
                ))
            })?;
            let nid: NodeID = match FromStr::from_str(&info.node_id) {
                Ok(nid) => nid,
                Err(err) => return Err(Status::internal(format!("{:#?}", err))),
            };
            let remote_rpc_channel = self.network.get_or_connect_lazy(remote_addr);

            Some(PutContext {
                progress: ProgressTracker::default(),
                remote_node_id: nid,
                remote_addr,
                remote_rpc_channel,
            })
        } else {
            None
        };

        Ok(ctx)
    }
}

#[async_trait]
impl<S> ConsistencyApi for ConsistencyService<S>
where
    S: Storage + Send + Sync + 'static,
{
    async fn put(
        &self,
        request: Request<PutPayload>,
    ) -> Result<Response<datacake_api::Timestamp>, Status> {
        let inner = request.into_inner();
        let doc = Document::from(inner.document.unwrap());
        let ctx = self.get_put_ctx(inner.ctx)?;

        self.group.clock().register_ts(doc.last_updated).await;

        let keyspace = self.group.get_or_create_keyspace(&inner.keyspace).await;
        let msg = crate::keyspace::Set {
            source: CONSISTENCY_SOURCE_ID,
            doc,
            ctx,
            _marker: PhantomData::<S>::default(),
        };

        if let Err(e) = keyspace.send(msg).await {
            error!(error = ?e, keyspace = keyspace.name(), "Failed to handle storage request on consistency API.");
            return Err(Status::internal(e.to_string()));
        };

        let ts = self.group.clock().get_time().await;
        Ok(Response::new(datacake_api::Timestamp::from(ts)))
    }

    async fn multi_put(
        &self,
        request: Request<MultiPutPayload>,
    ) -> Result<Response<datacake_api::Timestamp>, Status> {
        let inner = request.into_inner();
        let mut newest_ts = HLCTimestamp::new(0, 0, 0);
        let docs = inner
            .documents
            .into_iter()
            .map(Document::from)
            .map(|doc| {
                if doc.last_updated > newest_ts {
                    newest_ts = doc.last_updated;
                }
                doc
            })
            .collect::<Vec<_>>();
        let ctx = self.get_put_ctx(inner.ctx)?;

        self.group.clock().register_ts(newest_ts).await;

        let keyspace = self.group.get_or_create_keyspace(&inner.keyspace).await;
        let msg = crate::keyspace::MultiSet {
            source: CONSISTENCY_SOURCE_ID,
            docs,
            ctx,
            _marker: PhantomData::<S>::default(),
        };

        if let Err(e) = keyspace.send(msg).await {
            error!(error = ?e, keyspace = keyspace.name(), "Failed to handle storage request on consistency API.");
            return Err(Status::internal(e.to_string()));
        };

        let ts = self.group.clock().get_time().await;
        Ok(Response::new(datacake_api::Timestamp::from(ts)))
    }

    async fn remove(
        &self,
        request: Request<RemovePayload>,
    ) -> Result<Response<datacake_api::Timestamp>, Status> {
        let inner = request.into_inner();
        let document = inner.document.unwrap();
        let doc_id = document.id;
        let last_updated = HLCTimestamp::from(document.last_updated.unwrap());

        self.group.clock().register_ts(last_updated).await;

        let keyspace = self.group.get_or_create_keyspace(&inner.keyspace).await;
        let msg = crate::keyspace::Del {
            source: CONSISTENCY_SOURCE_ID,
            doc_id,
            ts: last_updated,
            _marker: PhantomData::<S>::default(),
        };

        if let Err(e) = keyspace.send(msg).await {
            error!(error = ?e, keyspace = keyspace.name(), "Failed to handle storage request on consistency API.");
            return Err(Status::internal(e.to_string()));
        };

        let ts = self.group.clock().get_time().await;
        Ok(Response::new(datacake_api::Timestamp::from(ts)))
    }

    async fn multi_remove(
        &self,
        request: Request<MultiRemovePayload>,
    ) -> Result<Response<datacake_api::Timestamp>, Status> {
        let inner = request.into_inner();

        let mut newest_ts = HLCTimestamp::new(0, 0, 0);
        let key_ts_pairs = inner
            .documents
            .into_iter()
            .map(|doc| {
                let ts = HLCTimestamp::from(doc.last_updated.unwrap());
                (doc.id, ts)
            })
            .map(|(id, ts)| {
                if ts > newest_ts {
                    newest_ts = ts;
                }
                (id, ts)
            })
            .collect::<Vec<_>>();

        self.group.clock().register_ts(newest_ts).await;

        let keyspace = self.group.get_or_create_keyspace(&inner.keyspace).await;
        let msg = crate::keyspace::MultiDel {
            source: CONSISTENCY_SOURCE_ID,
            key_ts_pairs,
            _marker: PhantomData::<S>::default(),
        };

        if let Err(e) = keyspace.send(msg).await {
            error!(error = ?e, keyspace = keyspace.name(), "Failed to handle storage request on consistency API.");
            return Err(Status::internal(e.to_string()));
        };

        let ts = self.group.clock().get_time().await;
        Ok(Response::new(datacake_api::Timestamp::from(ts)))
    }

    async fn apply_batch(
        &self,
        request: Request<BatchPayload>,
    ) -> Result<Response<datacake_api::Timestamp>, Status> {
        let inner = request.into_inner();
        let ts = HLCTimestamp::from(inner.timestamp.unwrap());
        self.group.clock().register_ts(ts).await;

        for remove_payload in inner.removed {
            self.multi_remove(Request::new(remove_payload)).await?;
        }

        for put_payload in inner.modified {
            self.multi_put(Request::new(put_payload)).await?;
        }

        let ts = self.group.clock().get_time().await;
        Ok(Response::new(datacake_api::Timestamp::from(ts)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::rpc::datacake_api::DocumentMetadata;
    use crate::test_utils::MemStore;

    #[tokio::test]
    async fn test_consistency_put() {
        static KEYSPACE: &str = "put-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ConsistencyService::new(group.clone(), RpcNetwork::default());

        let doc = Document::new(1, clock.get_time().await, b"Hello, world".to_vec());
        let put_req = Request::new(PutPayload {
            keyspace: KEYSPACE.to_string(),
            document: Some(doc.clone().into()),
            ctx: None,
        });

        service
            .put(put_req)
            .await
            .expect("Put request should succeed.");

        let saved_doc = storage
            .get(KEYSPACE, doc.id)
            .await
            .expect("Get new doc.")
            .expect("Doc should not be None");
        assert_eq!(saved_doc, doc, "Documents stored should match.");

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Iter metadata")
            .collect::<Vec<_>>();
        assert_eq!(metadata, vec![(doc.id, doc.last_updated, false)]);
    }

    #[tokio::test]
    async fn test_consistency_put_many() {
        static KEYSPACE: &str = "put-many-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ConsistencyService::new(group.clone(), RpcNetwork::default());

        let doc_1 = Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let doc_2 = Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());
        let put_req = Request::new(MultiPutPayload {
            keyspace: KEYSPACE.to_string(),
            ctx: None,
            documents: vec![
                doc_1.clone().into(),
                doc_2.clone().into(),
                doc_3.clone().into(),
            ],
        });

        service
            .multi_put(put_req)
            .await
            .expect("Put request should succeed.");

        let saved_docs = storage
            .multi_get(KEYSPACE, vec![doc_1.id, doc_2.id, doc_3.id].into_iter())
            .await
            .expect("Get new doc.")
            .collect::<Vec<_>>();
        assert_eq!(
            saved_docs,
            vec![doc_1.clone(), doc_2.clone(), doc_3.clone(),],
            "Documents stored should match."
        );

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Iter metadata")
            .collect::<HashSet<_>>();
        assert_eq!(
            metadata,
            HashSet::from_iter([
                (doc_1.id, doc_1.last_updated, false),
                (doc_2.id, doc_2.last_updated, false),
                (doc_3.id, doc_3.last_updated, false),
            ])
        );
    }

    #[tokio::test]
    async fn test_consistency_remove() {
        static KEYSPACE: &str = "remove-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ConsistencyService::new(group.clone(), RpcNetwork::default());

        let mut doc =
            Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        add_docs(KEYSPACE, vec![doc.clone()], &service).await;

        let saved_doc = storage
            .get(KEYSPACE, doc.id)
            .await
            .expect("Get new doc.")
            .expect("Doc should not be None");
        assert_eq!(saved_doc, doc, "Documents stored should match.");

        doc.last_updated = clock.get_time().await;
        let remove_req = Request::new(RemovePayload {
            keyspace: KEYSPACE.to_string(),
            document: Some(DocumentMetadata {
                id: doc.id,
                last_updated: Some(doc.last_updated.into()),
            }),
        });

        service.remove(remove_req).await.expect("Remove document.");

        let saved_doc = storage.get(KEYSPACE, doc.id).await.expect("Get new doc.");
        assert!(saved_doc.is_none(), "Documents should no longer exist.");

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Iter metadata")
            .collect::<Vec<_>>();
        assert_eq!(metadata, vec![(doc.id, doc.last_updated, true),]);
    }

    #[tokio::test]
    async fn test_consistency_remove_many() {
        static KEYSPACE: &str = "remove-many-keyspace";
        let group = KeyspaceGroup::<MemStore>::new_for_test().await;
        let clock = group.clock();
        let storage = group.storage();
        let service = ConsistencyService::new(group.clone(), RpcNetwork::default());

        let mut doc_1 =
            Document::new(1, clock.get_time().await, b"Hello, world 1".to_vec());
        let mut doc_2 =
            Document::new(2, clock.get_time().await, b"Hello, world 2".to_vec());
        let doc_3 = Document::new(3, clock.get_time().await, b"Hello, world 3".to_vec());
        add_docs(
            KEYSPACE,
            vec![doc_1.clone(), doc_2.clone(), doc_3.clone()],
            &service,
        )
        .await;

        let saved_docs = storage
            .multi_get(KEYSPACE, vec![doc_1.id, doc_2.id, doc_3.id].into_iter())
            .await
            .expect("Get new doc.")
            .collect::<Vec<_>>();
        assert_eq!(
            saved_docs,
            vec![doc_1.clone(), doc_2.clone(), doc_3.clone(),],
            "Documents stored should match."
        );

        doc_1.last_updated = clock.get_time().await;
        doc_2.last_updated = clock.get_time().await;
        let remove_req = Request::new(MultiRemovePayload {
            keyspace: KEYSPACE.to_string(),
            documents: vec![
                DocumentMetadata {
                    id: doc_1.id,
                    last_updated: Some(doc_1.last_updated.into()),
                },
                DocumentMetadata {
                    id: doc_2.id,
                    last_updated: Some(doc_2.last_updated.into()),
                },
            ],
        });

        service
            .multi_remove(remove_req)
            .await
            .expect("Remove documents.");

        let saved_docs = storage
            .multi_get(KEYSPACE, vec![doc_1.id, doc_2.id, doc_3.id].into_iter())
            .await
            .expect("Get new doc.")
            .collect::<Vec<_>>();
        assert_eq!(
            saved_docs,
            vec![doc_3.clone()],
            "Documents stored should match."
        );

        let metadata = storage
            .iter_metadata(KEYSPACE)
            .await
            .expect("Iter metadata")
            .collect::<HashSet<_>>();
        assert_eq!(
            metadata,
            HashSet::from_iter([
                (doc_1.id, doc_1.last_updated, true),
                (doc_2.id, doc_2.last_updated, true),
                (doc_3.id, doc_3.last_updated, false),
            ])
        );
    }

    async fn add_docs(
        keyspace: &str,
        docs: Vec<Document>,
        service: &ConsistencyService<MemStore>,
    ) {
        let put_req = Request::new(MultiPutPayload {
            keyspace: keyspace.to_string(),
            ctx: None,
            documents: docs.into_iter().map(|d| d.into()).collect(),
        });

        service
            .multi_put(put_req)
            .await
            .expect("Put request should succeed.");
    }
}
