#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[macro_use]
extern crate tracing;

use std::net::SocketAddr;

use anyhow::Result;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use clap::Parser;
use datacake::cluster::{
    ClusterOptions,
    ConnectionConfig,
    Consistency,
    DCAwareSelector,
    DatacakeCluster,
    DatacakeHandle,
};
use datacake_sled::SledStorage;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args: Args = Args::parse();
    let storage =
        datacake_sled::SledStorage::open(sled::Config::new().path(&args.data_dir))?;
    let node_1_id = age::x25519::Identity::generate();
    info!("using node_id {}", node_1_id.to_public().to_string());
    let connection_cfg = ConnectionConfig::new(
        args.cluster_listen_addr,
        args.public_addr.unwrap_or(args.cluster_listen_addr),
        args.seeds.into_iter(),
    );
    let cluster = DatacakeCluster::connect(
        node_1_id,
        connection_cfg,
        storage,
        DCAwareSelector::default(),
        ClusterOptions::default(),
    )
    .await?;
    use axum::error_handling::HandleErrorLayer;
    use tower::ServiceBuilder;
    use tower_http::trace::TraceLayer;
    let handle = cluster.handle();

    let app = Router::new()
        .route(
            "/:keyspace/:key",
            get(get_value).post(set_value).delete(remove_value),
        )
        .layer(
            ServiceBuilder::new()
                // Handle errors from middleware
                .layer(HandleErrorLayer::new(handle_error))
                .load_shed()
                .concurrency_limit(64)
                .timeout(std::time::Duration::from_secs(5))
                .layer(TraceLayer::new_for_http()),
        )
        .with_state(handle);

    info!("listening on {}", args.rest_listen_addr);
    let _ = axum::Server::bind(&args.rest_listen_addr)
        .serve(app.into_make_service())
        .await;

    cluster.shutdown().await;

    Ok(())
}
use axum::response::IntoResponse;
use axum::BoxError;
async fn handle_error(error: BoxError) -> impl IntoResponse {
    return (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("Something went wrong: {}", error),
    )
        .into_response();
}
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(long = "seed")]
    /// The set of seed nodes.
    ///
    /// This is used to kick start the auto-discovery of nodes within the cluster.
    seeds: Vec<String>,

    #[arg(long, default_value = "127.0.0.1:8000")]
    /// The address for the REST server to listen on.
    ///
    /// This is what will serve the API.
    rest_listen_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:8001")]
    /// The address for the cluster RPC system to listen on.
    cluster_listen_addr: SocketAddr,

    #[arg(long)]
    /// The public address for the node to broadcast to other nodes.
    ///
    /// If not provided the `cluster_listen_addr` is used which will only
    /// work when running a cluster on the same local network.
    public_addr: Option<SocketAddr>,

    #[arg(long)]
    /// The path to store the data.
    data_dir: String,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Params {
    keyspace: String,
    key: u64,
}

async fn get_value(
    Path(params): Path<Params>,
    State(handle): State<DatacakeHandle<SledStorage>>,
) -> Result<Bytes, StatusCode> {
    info!(
        doc_id = params.key,
        keyspace = params.keyspace,
        "Getting document!"
    );

    let doc = handle
        .get(&params.keyspace, params.key)
        .await
        .map_err(|e| {
            error!(error = ?e, doc_id = params.key, "Failed to fetch doc.");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    match doc {
        None => Err(StatusCode::NOT_FOUND),
        Some(doc) => Ok(doc.data),
    }
}

async fn set_value(
    Path(params): Path<Params>,
    State(handle): State<DatacakeHandle<SledStorage>>,
    data: Bytes,
) -> Result<Json<serde_json::Value>, StatusCode> {
    info!(
        doc_id = params.key,
        keyspace = params.keyspace,
        "Storing document!"
    );

    handle
        .put(&params.keyspace, params.key, data, Consistency::All)
        .await
        .map_err(|e| {
            error!(error = ?e, doc_id = params.key, "Failed to fetch doc.");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({
        "key": params.key,
        "keyspace": params.keyspace,
    })))
}

async fn remove_value(
    Path(params): Path<Params>,
    State(handle): State<DatacakeHandle<SledStorage>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    info!(
        doc_id = params.key,
        keyspace = params.keyspace,
        "Removing document!"
    );

    handle
        .del(&params.keyspace, params.key, Consistency::All)
        .await
        .map_err(|e| {
            error!(error = ?e, doc_id = params.key, "Failed to fetch doc.");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({
        "key": params.key,
        "keyspace": params.keyspace,
    })))
}
