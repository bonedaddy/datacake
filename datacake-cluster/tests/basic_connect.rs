use std::net::SocketAddr;
use std::time::Duration;

use datacake_cluster::test_utils::MemStore;
use datacake_cluster::{
    ClusterOptions,
    ConnectionConfig,
    DCAwareSelector,
    DatacakeCluster,
};

#[tokio::test]
async fn test_basic_connect() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "trace,sled=info");
    let _ = tracing_subscriber::fmt::try_init();

    let addr = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());
    let node_id = age::x25519::Identity::generate();
    let cluster = DatacakeCluster::connect(
        node_id,
        connection_cfg,
        MemStore::default(),
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    cluster.shutdown().await;

    Ok(())
}
