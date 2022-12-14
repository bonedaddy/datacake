use std::error::Error;
use std::fmt::Debug;
use std::net::SocketAddr;

use thiserror::Error;

use crate::nodes_selector::ConsistencyError;
use crate::storage::BulkMutationError;

#[derive(Debug, Error)]
pub enum DatacakeError<E: Error + Send + 'static> {
    #[error("{0}")]
    /// An error has occurred within Chitchat.
    ChitChatError(String),

    #[error("An unknown error occurred during the operation: {0}")]
    /// An error has occurred which datacake was not expecting nad from
    /// an unknown source.
    UnknownError(anyhow::Error),

    #[error(
        "A failure occurred within the user provided `DataStore` implementation: {0}"
    )]
    /// A failure has occurred within the provided storage system causing the operation
    /// to fail.
    StorageError(#[from] E),

    #[error(
        "A failure occurred within the user provided `DataStore` implementation on a bulk operation: {0}"
    )]
    /// A failure has occurred within the provided storage system causing the operation
    /// to fail.
    ///
    /// This error however, includes the set of doc ids which *were* successfully completed,
    /// this can be used to maintain partial and incremental updates despite and error, otherwise
    /// bulk storage operations must be entirely atomic if they do not specify the successful IDs.
    BulkStorageError(#[from] BulkMutationError<E>),

    #[error("Failed to complete operation due to consistency level failure: {0}")]
    /// The operation succeeded on the local node but failed to meet the required
    /// consistency level within the timeout period. (2 seconds)
    ConsistencyError(ConsistencyError),

    #[error("Transport Error: ({0}) - {1}")]
    /// An error occurred when attempting to open a connection or listen on a given address.
    TransportError(SocketAddr, tonic::transport::Error),

    #[error("Rpc Error: ({0}) - {1}")]
    /// An error occurred during RPC communication with other nodes.
    RpcError(SocketAddr, tonic::Status),
    #[error("connection timed out")]
    Timeout,
}
