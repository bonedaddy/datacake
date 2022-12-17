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
    std::env::set_var("RUST_LOG", "debug,h2=info,tower=info,sled=info");
    let _ = tracing_subscriber::fmt::try_init();
    let node_1_id = age::x25519::Identity::generate();
    let node_2_id = age::x25519::Identity::generate();
    let store_1 = SledStorage::open_temporary()?;
    let store_2 = SledStorage::open_temporary()?;

    let addr_1 = "127.0.0.1:9000".parse::<SocketAddr>().unwrap();
    let connection_cfg_1 = ConnectionConfig::new(addr_1, addr_1, Vec::<String>::new());

    let addr_2 = "127.0.0.1:9001".parse::<SocketAddr>().unwrap();
    let connection_cfg_2 =
        ConnectionConfig::new(addr_2, addr_2, vec!["127.0.0.1:9000".to_string()]);

    let cluster_1 = DatacakeCluster::connect(
        node_1_id.clone(),
        connection_cfg_1,
        store_1,
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await?;

    let cluster_2 = DatacakeCluster::connect(
        node_2_id.clone(),
        connection_cfg_2,
        store_2,
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let handle_1 = cluster_1.handle();
    let handle_2 = cluster_2.handle();

    handle_1
        .put(
            KEYSPACE,
            1,
            b"Hello, world".to_vec(),
            Consistency::EachQuorum,
        )
        .await
        .expect("Put value.");

    let doc = handle_1
        .get(KEYSPACE, 1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id, 1);
    assert_eq!(doc.data.as_ref(), b"Hello, world");

    let doc = handle_2
        .get(KEYSPACE, 1)
        .await
        .expect("Get value.")
        .expect("Document should not be none");
    assert_eq!(doc.id, 1);
    assert_eq!(doc.data.as_ref(), b"Hello, world");

    handle_2
        .del(KEYSPACE, 1, Consistency::EachQuorum)
        .await
        .expect("Del value.");

    let doc = handle_1.get(KEYSPACE, 1).await.expect("Get value.");

    assert!(doc.is_none(), "No document should not exist!");

    handle_1
        .del(KEYSPACE, 2, Consistency::EachQuorum)
        .await
        .expect("Del value which doesnt exist locally.");
    let doc = handle_1.get(KEYSPACE, 2).await.expect("Get value.");
    assert!(doc.is_none(), "No document should not exist!");

    cluster_1.shutdown().await;
    cluster_2.shutdown().await;

    Ok(())
}
