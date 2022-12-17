use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tonic::transport::{Channel, Endpoint, Error};

use crate::node_identifier::NodeID;

pub const TIMEOUT_LIMIT: Duration = Duration::from_secs(4);
pub const CONNECT_TIMEOUT_LIMIT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct RpcNetworkClient {
    pub(crate) channel: Channel,
}

#[derive(Clone, Default)]
/// A collection of RPC client connections which can be reused and multiplexed.
pub struct RpcNetwork {
    clients: Arc<RwLock<HashMap<SocketAddr, RpcNetworkClient>>>,
}

impl RpcNetwork {
    /// Attempts to get an already existing connection or creates a new connection.
    pub async fn get_or_connect(&self, addr: SocketAddr) -> Result<Channel, Error> {
        {
            let guard = self.clients.read();
            if let Some(client) = guard.get(&addr) {
                return Ok(client.channel.clone());
            }
        }

        trace!(addr = %addr, "Connect client to network.");
        self.connect(addr).await
    }

    /// Connects to a given address and adds it to the clients.
    pub async fn connect(&self, addr: SocketAddr) -> Result<Channel, Error> {
        let uri = format!("http://{}", addr);
        let channel = Endpoint::from_str(&uri)
            .unwrap()
            .timeout(TIMEOUT_LIMIT)
            .connect_timeout(CONNECT_TIMEOUT_LIMIT)
            .connect()
            .await?;

        {
            let mut guard = self.clients.write();
            guard.insert(
                addr,
                RpcNetworkClient {
                    channel: channel.clone(),
                },
            );
        }

        Ok(channel)
    }
    /// Attempts to get an already existing connection or creates a new connection.
    pub fn get_or_connect_lazy(&self, addr: SocketAddr) -> Channel {
        {
            let guard = self.clients.read();
            if let Some(client) = guard.get(&addr) {
                return client.channel.clone();
            }
        }

        self.connect_lazy(addr)
    }

    /// Creates a new endpoint channel which connects lazily to the node.
    pub fn connect_lazy(&self, addr: SocketAddr) -> Channel {
        let uri = format!("http://{}", addr);
        let channel = Endpoint::from_str(&uri)
            .unwrap()
            .timeout(TIMEOUT_LIMIT)
            .connect_timeout(CONNECT_TIMEOUT_LIMIT)
            .connect_lazy();

        {
            let mut guard = self.clients.write();
            guard.insert(
                addr,
                RpcNetworkClient {
                    channel: channel.clone(),
                },
            );
        }

        channel
    }

    /// Removes a client from the network.
    pub fn disconnect(&self, addr: SocketAddr) {
        let mut guard = self.clients.write();
        guard.remove(&addr);
    }
}
