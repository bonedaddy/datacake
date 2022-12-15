use std::net::SocketAddr;

use anyhow::Result;
use datacake_cluster::{
    ClusterOptions,
    ConnectionConfig,
    Consistency,
    DCAwareSelector,
    DatacakeCluster,
};
use datacake_sled::SledStorage;

static KEYSPACE: &str = "sqlite-store";

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_sled_cluster() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    let _ = tracing_subscriber::fmt::try_init();

    let store = SledStorage::open_temporary()?;

    let addr = "127.0.0.1:9000".parse::<SocketAddr>().unwrap();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());

    let cluster = DatacakeCluster::connect(
        "node-1",
        connection_cfg,
        store,
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await?;

    let handle = cluster.handle();

    handle
        .put(KEYSPACE, 1, b"Hello, world".to_vec(), Consistency::EachQuorum)
        .await
        .expect("Put value.");

    let doc = handle
        .get(KEYSPACE, 1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id, 1);
    assert_eq!(doc.data.as_ref(), b"Hello, world");
    log::info!("deleting key");
    handle
        .del(KEYSPACE, 1, Consistency::EachQuorum)
        .await
        .expect("Del value.");
    log::info!("getting key");
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    let doc = handle.get(KEYSPACE, 1).await.expect("Get value.");
    log::info!("got doc {:#?}", doc);
    assert!(doc.is_none(), "No document should not exist!");

    handle
        .del(KEYSPACE, 2, Consistency::EachQuorum)
        .await
        .expect("Del value which doesnt exist locally.");
    let doc = handle.get(KEYSPACE, 2).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    cluster.shutdown().await;

    Ok(())
}
