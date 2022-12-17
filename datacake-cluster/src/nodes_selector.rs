use std::borrow::Cow;
use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

const NODE_CACHE_TIMEOUT: Duration = Duration::from_secs(2);

/// Starts the node selector actor with a given node selector implementation.
///
/// This system will cache selected nodes for a given consistency level for upto 2 seconds.
pub async fn start_node_selector<S>(
    local_node: SocketAddr,
    local_dc: Cow<'static, str>,
    mut selector: S,
) -> NodeSelectorHandle
where
    S: NodeSelector + Send + 'static,
{
    let (tx, rx) = flume::bounded(100);

    tokio::spawn(async move {
        let mut total_nodes = 0;
        let mut data_centers = BTreeMap::new();
        let mut cached_nodes = HashMap::<Consistency, (Instant, Vec<(String, SocketAddr)>)>::new();

        while let Ok(op) = rx.recv_async().await {
            match op {
                Op::SetNodes {
                    data_centers: new_data_centers,
                } => {
                    let mut new_total = 0;
                    for (name, nodes) in new_data_centers {
                        new_total += nodes.len();
                        data_centers.insert(name, NodeCycler::from(nodes));
                    }
                    total_nodes = new_total;
                    info!(
                        total_nodes = total_nodes,
                        num_data_centers = data_centers.len(),
                        "Node selector has updated eligible nodes.",
                    );

                    cached_nodes.clear();
                },
                Op::GetNodes { consistency, tx } => {
                    if let Some((last_refreshed, nodes)) = cached_nodes.get(&consistency)
                    {
                        if last_refreshed.elapsed() < NODE_CACHE_TIMEOUT {
                            let _ = tx.send(Ok(nodes.clone()));
                            continue;
                        }
                    }

                    let nodes = selector.select_nodes(
                        local_node,
                        &local_dc,
                        total_nodes,
                        &mut data_centers,
                        consistency,
                    );

                    if let Ok(ref nodes) = nodes {
                        cached_nodes
                            .insert(consistency, (Instant::now(), nodes.clone()));
                    }

                    let _ = tx.send(nodes);
                },
            }
        }

        info!("Node selector service has shutdown.");
    });

    NodeSelectorHandle { tx }
}

#[derive(Clone)]
/// A handle to the node selector actor responsible for working out
/// what replicas should be prioritized when sending events based on
/// a given consistency level.
pub struct NodeSelectorHandle {
    tx: flume::Sender<Op>,
}

impl NodeSelectorHandle {
    /// Set the nodes which can be used by the selector.
    pub async fn set_nodes(
        &self,
        data_centers: BTreeMap<Cow<'static, str>, Vec<(String, SocketAddr)>>,
    ) {
        self.tx
            .send_async(Op::SetNodes { data_centers })
            .await
            .expect("contact actor");
    }

    /// Gets a set of nodes based on a given consistency level.
    ///
    /// If the consistency level cannot be met with the given data centers
    /// a [ConsistencyError] is returned.
    pub async fn get_nodes(
        &self,
        consistency: Consistency,
    ) -> Result<Vec<(String, SocketAddr)>, ConsistencyError> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::GetNodes { consistency, tx })
            .await
            .expect("contact actor");

        rx.await.expect("get actor response")
    }
}

enum Op {
    SetNodes {
        data_centers: BTreeMap<Cow<'static, str>, Vec<(String, SocketAddr)>>,
    },
    GetNodes {
        consistency: Consistency,
        tx: oneshot::Sender<Result<Vec<(String, SocketAddr)>, ConsistencyError>>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ConsistencyError {
    #[error(
        "Not enough nodes are present in the cluster to achieve this consistency level."
    )]
    NotEnoughNodes { live: usize, required: usize },

    #[error(
        "Failed to achieve the desired consistency level before the timeout \
        ({timeout:?}) elapsed. Got {responses} responses but needed {required} responses."
    )]
    ConsistencyFailure {
        responses: usize,
        required: usize,
        timeout: Duration,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
/// The consistency level which should be reached before a write can be
/// returned as successful.
///
/// If the consistency level is not met then an error should be returned,
/// but this does not mean that the operation has failed to go through on
/// all nodes, it simply means that the target number of replicas has not
/// been reached.
pub enum Consistency {
    /// No other replicas will have the operation broadcast to them.
    None,

    /// Broadcast the change to one replica as determined by the `NodeSelector`.
    One,

    /// Broadcast the change to two replicas as determined by the `NodeSelector`.
    Two,

    /// Broadcast the change to three replicas as determined by the `NodeSelector`.
    Three,

    /// A simple majority of all replicas across all data centers.
    ///
    /// The nodes selected are determined by the node selector, this is designed
    /// to be a hint to the node selector rather than a fixed rule.
    Quorum,

    /// A simple majority of in the local data center.
    ///
    /// The nodes selected are determined by the node selector, this is designed
    /// to be a hint to the node selector rather than a fixed rule.
    LocalQuorum,

    /// The change is broadcast to all nodes as part of the cluster.
    All,

    /// A simple majority in each data center.
    ///
    /// If no data center is provided to any nodes when they're created, this
    /// is the same as [Consistency::Quorum].
    ///
    /// The nodes selected are determined by the node selector, this is designed
    /// to be a hint to the node selector rather than a fixed rule.
    EachQuorum,
}

pub trait NodeSelector {
    /// Produces a set of node addresses based on the desired consistency level.
    ///
    /// A set of `nodes` are provided with a mapping of `data_center -> node_addresses`.
    fn select_nodes(
        &mut self,
        local_node: SocketAddr,
        local_dc: &str,
        total_nodes: usize,
        data_centers: &mut BTreeMap<Cow<'static, str>, NodeCycler>,
        consistency: Consistency,
    ) -> Result<Vec<(String, SocketAddr)>, ConsistencyError>;
}

#[derive(Debug, Copy, Clone, Default)]
/// A data-center aware node selector.
///
/// This will prioritise sending replication data to nodes which are part
/// of a different availability zone or data center center as it's referred to.
///
/// If this is not possible or can distribute the load evenly, it may also
/// choose nodes apart of the same data center.
pub struct DCAwareSelector;

impl NodeSelector for DCAwareSelector {
    fn select_nodes(
        &mut self,
        local_node: SocketAddr,
        local_dc: &str,
        total_nodes: usize,
        data_centers: &mut BTreeMap<Cow<'static, str>, NodeCycler>,
        consistency: Consistency,
    ) -> Result<Vec<(String, SocketAddr)>, ConsistencyError> {
        let mut selected_nodes = Vec::new();

        match consistency {
            Consistency::One => {
                return select_n_nodes(
                    local_node,
                    local_dc,
                    1,
                    total_nodes,
                    data_centers,
                )
            },
            Consistency::Two => {
                return select_n_nodes(
                    local_node,
                    local_dc,
                    2,
                    total_nodes,
                    data_centers,
                )
            },
            Consistency::Three => {
                return select_n_nodes(
                    local_node,
                    local_dc,
                    3,
                    total_nodes,
                    data_centers,
                )
            },
            Consistency::Quorum => {
                // The majority is not `(len / 2) + 1` here as the local node will also
                // be setting the value, giving us out `n + 1` majority.
                let majority = total_nodes / 2;

                let mut dcs_iterators = data_centers
                    .iter()
                    .map(|(_, nodes)| {
                        nodes
                            .get_nodes()
                            .iter()
                            .cloned()
                            .filter(|(_, addr)| addr != &local_node)
                    })
                    .collect::<Vec<_>>();
                let mut previous_total = selected_nodes.len();
                while selected_nodes.len() < majority {
                    let nodes = dcs_iterators.iter_mut().filter_map(|iter| iter.next());
                    selected_nodes.extend(nodes);

                    // We have no more nodes to add.
                    if previous_total == selected_nodes.len() {
                        return Err(ConsistencyError::NotEnoughNodes {
                            live: selected_nodes.len(),
                            required: majority,
                        });
                    }

                    previous_total = selected_nodes.len();
                }
            },
            Consistency::LocalQuorum => {
                if let Some(nodes) = data_centers.get(local_dc) {
                    // The majority is not `(len / 2) + 1` here as the local node will also
                    // be setting the value, giving us out `n + 1` majority.
                    let majority = nodes.len() / 2;
                    selected_nodes.extend(
                        nodes
                            .get_nodes()
                            .iter()
                            .cloned()
                            .filter(|(_, addr)| addr != &local_node)
                            .take(majority),
                    );
                }
            },
            Consistency::All => selected_nodes.extend(
                data_centers
                    .values()
                    .flat_map(|cycler| cycler.nodes.clone())
                    .filter(|(_, addr)| addr != &local_node),
            ),
            Consistency::EachQuorum => {
                for (name, nodes) in data_centers {
                    let majority = if name == local_dc {
                        // The majority is not `(len / 2) + 1` here as the local node will also
                        // be setting the value, giving us out `n + 1` majority.
                        nodes.len() / 2
                    } else {
                        (nodes.len() / 2) + 1
                    };

                    selected_nodes.extend(
                        nodes
                            .get_nodes()
                            .iter()
                            .cloned()
                            .filter(|(_, addr)| addr != &local_node)
                            .take(majority),
                    );
                }
            },
            Consistency::None => {},
        }

        Ok(selected_nodes)
    }
}

#[instrument(name = "dc-aware-selector")]
/// Selects `n` nodes, prioritising nodes not apart of the local data center.
///
/// The system will attempt to distribute nodes across as many data centers as it can
/// within reason.
///
/// ### Selection Behaviour
///
/// Lets say we have the following cluster:
/// ```ignore
/// DC1: [192.168.0.1, 192.168.0.2, 192.168.0.3]
/// DC2: [192.168.0.4, 192.168.0.5]
/// DC3: [192.168.0.6]
/// ```
///
/// And we want to get `3` replicas with this DC set. Our DC will be `DC1`.
///
/// We first work out if we are able to avoid our own DC (`DC1`), we do this because
/// the system assumes that nodes on the same DC are part of the same availability zone,
/// meaning if something happens, e.g. a hardware failure, all nodes in that DC will be down.
///
/// We determine if we can do this by asking if the number of DCs available to us is greater than
/// `1`, in this case it is. Great!
///
/// Next we select our data centers to select the nodes from:
/// - If the number of DCs is *greater than* the `n` nodes selected, we randomly pick `n`
///   DCs from out set.
/// - If the number of DCs is *equal to* the `n` nodes selected, we select all DCs, and take a node
///   out of each DC.
/// - If the number of DCs is *less than* the `n` nodes, we select all DCs and work out how many
///   nodes short we will be if we took 1 node from each DC.
///
/// Once we have our DCs selected, and the number of additional nodes to fetch, we select ours nodes:
/// - Each DC has one node selected and added to the set. If the node happens to be the current/local node,
///   we fetch the next available node.
/// - For each DC we go to, we select a number of nodes so that we evenly (or as evenly as possible)
///   select an additional node from each DC until we have satisfied the `n` number of nodes.
fn select_n_nodes(
    local_node: SocketAddr,
    local_dc: &str,
    n: usize,
    total_nodes: usize,
    data_centers: &mut BTreeMap<Cow<'static, str>, NodeCycler>,
) -> Result<Vec<(String, SocketAddr)>, ConsistencyError> {
    use rand::seq::IteratorRandom;
    let mut rng = rand::thread_rng();

    let num_nodes_outside_dc = total_nodes
        - data_centers
            .get(local_dc)
            .map(|nodes| nodes.len())
            .unwrap_or_default();
    let can_skip_local_dc = num_nodes_outside_dc >= n;

    let num_data_centers = if can_skip_local_dc {
        data_centers.len() - 1
    } else {
        data_centers.len()
    };

    let mut num_extra_nodes = 0;
    let selected_dcs = if num_data_centers <= n {
        num_extra_nodes = n - num_data_centers;
        data_centers
            .iter_mut()
            .filter(|(dc, _)| !(can_skip_local_dc && (dc.as_ref() == local_dc)))
            .collect::<Vec<_>>()
    } else {
        data_centers
            .iter_mut()
            .filter(|(dc, _)| !(can_skip_local_dc && (dc.as_ref() == local_dc)))
            .choose_multiple(&mut rng, n)
    };

    let mut dc_count = selected_dcs.len();
    let mut selected_nodes = Vec::new();
    for (_, dc_nodes) in selected_dcs.into_iter() {
        let node = match dc_nodes.next() {
            Some((node_id, node)) => {
                if node == local_node {
                    if dc_nodes.len() <= 1 {
                        num_extra_nodes += 1;
                        dc_count -= 1;
                        continue;
                    }

                    dc_nodes.next().unwrap()
                } else {
                    (node_id, node)
                }
            },
            // In theory this should never happen, but we handle it just in case.
            None => {
                num_extra_nodes += 1;
                dc_count -= 1;
                continue;
            },
        };

        selected_nodes.push(node);

        if num_extra_nodes == 0 {
            continue;
        }

        let num_extra_nodes_per_dc = num_extra_nodes / cmp::max(dc_count - 1, 1);
        for _ in 0..num_extra_nodes_per_dc {
            if let Some(node) = dc_nodes.next() {
                if selected_nodes.contains(&node) {
                    continue;
                }

                selected_nodes.push(node);
                num_extra_nodes -= 1;
            }
        }

        dc_count -= 1;
    }

    if selected_nodes.len() >= n {
        debug!(selected_node = ?selected_nodes, "Nodes have been selected for the given parameters.");
        Ok(selected_nodes)
    } else {
        warn!(
            live_nodes = total_nodes - 1,
            required_node = n,
            "Failed to meet consistency level due to shortage of live nodes"
        );
        Err(ConsistencyError::NotEnoughNodes {
            live: selected_nodes.len(),
            required: n,
        })
    }
}

#[derive(Debug)]
pub struct NodeCycler {
    cursor: usize,
    nodes: Vec<(String, SocketAddr)>,
}

impl NodeCycler {
    /// Extends the set of nodes for the cycler.
    pub fn extend(&mut self, iter: impl Iterator<Item = (String, SocketAddr)>) {
        self.nodes.extend(iter);
    }

    /// Gets a mutable reference to the inner nodes buffer.
    pub fn get_nodes_mut(&mut self) -> &mut Vec<(String, SocketAddr)> {
        &mut self.nodes
    }

    /// Gets a immutable reference to the inner nodes buffer.
    pub fn get_nodes(&self) -> &Vec<(String, SocketAddr)> {
        &self.nodes
    }

    #[inline]
    /// Gets the number of nodes in the cycler.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}

impl From<Vec<(String, SocketAddr)>> for NodeCycler {
    fn from(nodes: Vec<(String, SocketAddr)>) -> Self {
        Self { cursor: 0, nodes }
    }
}

impl Iterator for NodeCycler {
    type Item = (String, SocketAddr);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.nodes.len() {
            self.cursor = 0;
        }

        let res = self.nodes.get(self.cursor).cloned();

        self.cursor += 1;

        res
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::fmt::Display;
    use std::net::{IpAddr, SocketAddr};

    use crate::nodes_selector::{
        select_n_nodes,
        Consistency,
        DCAwareSelector,
        NodeCycler,
        NodeSelector,
    };

    #[test]
    fn test_dc_aware_selector() {
        let addr = make_addr(0, 0);
        let total_nodes = 6;
        let mut dc = make_dc(vec![3, 2, 1]);
        let mut selector = DCAwareSelector;

        let nodes = selector
            .select_nodes(addr, "dc-0", total_nodes, &mut dc, Consistency::All)
            .expect("Get nodes");
        assert_eq!(
            nodes.len(),
            total_nodes - 1,
            "Expected all nodes to be selected except for local node."
        );

        let nodes = selector
            .select_nodes(addr, "dc-0", total_nodes, &mut dc, Consistency::None)
            .expect("Get nodes");
        assert!(nodes.is_empty(), "Expected no nodes to be selected.");

        let nodes = selector
            .select_nodes(addr, "dc-0", total_nodes, &mut dc, Consistency::EachQuorum)
            .expect("Get nodes");
        assert_eq!(
            nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(),
            vec![
                make_addr(0, 1),
                make_addr(1, 0),
                make_addr(1, 1),
                make_addr(2, 0),
            ]
        );

        let nodes = selector
            .select_nodes(addr, "dc-0", total_nodes, &mut dc, Consistency::LocalQuorum)
            .expect("Get nodes");
        assert_eq!(nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(), vec![make_addr(0, 1)]);

        let nodes = selector
            .select_nodes(addr, "dc-0", total_nodes, &mut dc, Consistency::Quorum)
            .expect("Get nodes");
        assert_eq!(
            nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(),
            vec![make_addr(0, 1), make_addr(1, 0), make_addr(2, 0),]
        );

        let mut dc = make_dc(vec![1]);
        selector
            .select_nodes(addr, "dc-0", total_nodes, &mut dc, Consistency::One)
            .expect_err("Node selector should reject consistency level.");
        let mut dc = make_dc(vec![2]);
        selector
            .select_nodes(addr, "dc-0", total_nodes, &mut dc, Consistency::Two)
            .expect_err("Node selector should reject consistency level.");
        let mut dc = make_dc(vec![1, 1]);
        selector
            .select_nodes(addr, "dc-0", total_nodes, &mut dc, Consistency::Two)
            .expect_err("Node selector should reject consistency level.");
        let mut dc = make_dc(vec![1, 1, 1]);
        selector
            .select_nodes(addr, "dc-0", total_nodes, &mut dc, Consistency::Three)
            .expect_err("Node selector should reject consistency level.");
        let mut dc = make_dc(vec![2, 1]);
        selector
            .select_nodes(addr, "dc-0", total_nodes, &mut dc, Consistency::Three)
            .expect_err("Node selector should reject consistency level.");
    }

    #[test]
    fn test_select_n_nodes_equal_dc_count() {
        let addr = make_addr(0, 0);
        let total_nodes = 6;
        let mut dc = make_dc(vec![3, 2, 1]);

        // DC-0
        let nodes =
            select_n_nodes(addr, "dc-0", 3, total_nodes, &mut dc).expect("get nodes");
        assert_eq!(
            nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(),
            vec![make_addr(1, 0), make_addr(1, 1), make_addr(2, 0),],
        );

        let nodes =
            select_n_nodes(addr, "dc-0", 2, total_nodes, &mut dc).expect("get nodes");
        assert_eq!(nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(), vec![make_addr(1, 0), make_addr(2, 0),],);

        let nodes =
            select_n_nodes(addr, "dc-0", 0, total_nodes, &mut dc).expect("get nodes");
        assert_eq!(nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(), Vec::<SocketAddr>::new());

        // DC-1
        let nodes =
            select_n_nodes(addr, "dc-1", 3, total_nodes, &mut dc).expect("get nodes");
        assert_eq!(
            nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(),
            vec![make_addr(0, 1), make_addr(0, 2), make_addr(2, 0),],
        );

        let nodes =
            select_n_nodes(addr, "dc-1", 2, total_nodes, &mut dc).expect("get nodes");
        assert_eq!(nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(), vec![make_addr(0, 1), make_addr(2, 0),],);

        let nodes =
            select_n_nodes(addr, "dc-1", 0, total_nodes, &mut dc).expect("get nodes");
        assert_eq!(nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(), Vec::<SocketAddr>::new());

        // DC-2
        let nodes =
            select_n_nodes(addr, "dc-2", 3, total_nodes, &mut dc).expect("get nodes");
        assert_eq!(
            nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(),
            vec![make_addr(0, 2), make_addr(0, 0), make_addr(1, 1),],
        );

        let nodes =
            select_n_nodes(addr, "dc-2", 2, total_nodes, &mut dc).expect("get nodes");
        assert_eq!(nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(), vec![make_addr(0, 1), make_addr(1, 0),],);

        let nodes =
            select_n_nodes(addr, "dc-2", 0, total_nodes, &mut dc).expect("get nodes");
        assert_eq!(nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(), Vec::<SocketAddr>::new());
    }

    #[test]
    fn test_select_n_nodes_less_dc_count() {
        let addr = make_addr(0, 0);
        let total_nodes = 5;
        let mut dc = make_dc(vec![3, 2]);

        // DC-0
        let nodes =
            select_n_nodes(addr, "dc-0", 3, total_nodes, &mut dc).expect("get nodes");
        assert_eq!(
            nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(),
            vec![make_addr(0, 1), make_addr(0, 2), make_addr(1, 0),],
        );

        let nodes =
            select_n_nodes(addr, "dc-0", 2, total_nodes, &mut dc).expect("get nodes");
        assert_eq!(nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(), vec![make_addr(1, 1), make_addr(1, 0),],);

        let nodes =
            select_n_nodes(addr, "dc-0", 0, total_nodes, &mut dc).expect("get nodes");
        assert_eq!(nodes.iter().map(|(_, addr)| addr.clone()).collect::<Vec<_>>(), Vec::<SocketAddr>::new());
    }

    fn make_dc(distribution: Vec<usize>) -> BTreeMap<Cow<'static, str>, NodeCycler> {
        let mut dc = BTreeMap::new();
        for (dc_n, num_nodes) in distribution.into_iter().enumerate() {
            let name = to_dc_name(dc_n);

            let mut nodes = Vec::new();
            for i in 0..num_nodes {
                let addr = make_addr(dc_n as u8, i as u8);
                // todo: decide how to set the node name here
                nodes.push((age::x25519::Identity::generate().to_public().to_string(), addr));
            }

            dc.insert(name, NodeCycler::from(nodes));
        }

        dc
    }

    fn make_addr(dc_id: u8, node_n: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::from([127, dc_id, 0, node_n]), 80)
    }

    fn to_dc_name(dc: impl Display) -> Cow<'static, str> {
        Cow::Owned(format!("dc-{}", dc))
    }
}
