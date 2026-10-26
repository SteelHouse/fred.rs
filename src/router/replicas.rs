use crate::{
  error::Error,
  modules::inner::ClientInner,
  protocol::{command::Command, connection, connection::Connection},
  runtime::RefCount,
  types::config::Server,
};
use bytes_utils::Str;
use futures::future::join_all;
use std::{
  collections::{HashMap, VecDeque},
  fmt,
  fmt::Formatter,
};

#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use crate::types::config::TlsHostMapping;

/// An interface used to filter the list of available replica nodes.
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
#[async_trait]
pub trait ReplicaFilter: Send + Sync + 'static {
  /// Returns whether the replica node mapping can be used when routing commands to replicas.
  #[allow(unused_variables)]
  async fn filter(&self, primary: &Server, replica: &Server) -> bool {
    true
  }
}

/// How the client chooses the node that serves a command sent to replicas.
///
/// The zone-aware variants compare the client's zone with the `availability_zone` each node reports in `INFO server`
/// (Valkey 8.0+). Nodes that do not report a zone never match.
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ReadFrom {
  /// Round-robin across the replicas of the shard.
  #[default]
  AnyReplica,
  /// Round-robin across the replicas in the provided zone, falling back to any connected replica.
  AzAffinity(Str),
  /// Round-robin across the replicas in the provided zone, then use the primary if it is in that zone, falling back
  /// to any connected replica.
  ///
  /// With one node of each shard in each zone, this serves every read from the client's zone.
  AzAffinityReplicasAndPrimary(Str),
}

impl ReadFrom {
  /// The zone the client prefers, if any.
  pub fn zone(&self) -> Option<&Str> {
    match self {
      ReadFrom::AnyReplica => None,
      ReadFrom::AzAffinity(zone) | ReadFrom::AzAffinityReplicasAndPrimary(zone) => Some(zone),
    }
  }
}

/// Configuration options for replica node connections.
///
/// When connecting to a replica the client will use the parameters specified in the
/// [ReconnectPolicy](crate::types::config::ReconnectPolicy).
///
/// Currently only clustered replicas are supported.
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
#[derive(Clone)]
pub struct ReplicaConfig {
  /// Whether the client should lazily connect to replica nodes.
  ///
  /// Ignored by the zone-aware [ReadFrom] variants, which connect eagerly since a replica's zone is only known once
  /// the client has connected to it.
  ///
  /// Default: `true`
  pub lazy_connections:           bool,
  /// How the client chooses among the nodes of a shard.
  ///
  /// Default: [ReadFrom::AnyReplica]
  pub read_from:                  ReadFrom,
  /// An optional interface for filtering available replica nodes.
  ///
  /// Default: `None`
  pub filter:                     Option<RefCount<dyn ReplicaFilter>>,
  /// Whether the client should ignore errors from replicas that occur when the max reconnection count is reached.
  ///
  /// This implies `primary_fallback: true`.
  ///
  /// Default: `true`
  pub ignore_reconnection_errors: bool,
  /// Whether the client should use the associated primary node if no replica exists that can serve a command.
  ///
  /// Default: `true`
  pub primary_fallback:           bool,
}

impl fmt::Debug for ReplicaConfig {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("ReplicaConfig")
      .field("lazy_connections", &self.lazy_connections)
      .field("read_from", &self.read_from)
      .field("ignore_reconnection_errors", &self.ignore_reconnection_errors)
      .field("primary_fallback", &self.primary_fallback)
      .finish()
  }
}

impl PartialEq for ReplicaConfig {
  fn eq(&self, other: &Self) -> bool {
    self.lazy_connections == other.lazy_connections
      && self.read_from == other.read_from
      && self.ignore_reconnection_errors == other.ignore_reconnection_errors
      && self.primary_fallback == other.primary_fallback
  }
}

impl Eq for ReplicaConfig {}

impl Default for ReplicaConfig {
  fn default() -> Self {
    ReplicaConfig {
      lazy_connections:           true,
      read_from:                  ReadFrom::AnyReplica,
      filter:                     None,
      ignore_reconnection_errors: true,
      primary_fallback:           true,
    }
  }
}

impl ReplicaConfig {
  /// Whether replica connections are created on first use.
  pub(crate) fn lazy(&self) -> bool {
    self.lazy_connections && self.read_from.zone().is_none()
  }
}

/// A container for round-robin routing among replica nodes.
// This implementation optimizes for next() at the cost of add() and remove()
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
pub struct ReplicaRouter {
  counter: usize,
  servers: Vec<Server>,
}

impl ReplicaRouter {
  /// Read the server that should receive the next command.
  pub fn next(&mut self) -> Option<&Server> {
    self.counter = (self.counter + 1) % self.servers.len();
    self.servers.get(self.counter)
  }

  /// Read the server that should receive the next command from a client in `zone`: a connected replica in `zone`,
  /// otherwise any connected replica, otherwise the next replica in round-robin order.
  ///
  /// `connected_zone` returns `None` for a replica without a connection, otherwise the zone its connection reported.
  pub fn next_in_zone<'a>(
    &mut self,
    zone: &Str,
    connected_zone: impl Fn(&Server) -> Option<Option<&'a Str>>,
  ) -> Option<&Server> {
    let idx = self
      .find_next(|server| connected_zone(server) == Some(Some(zone)))
      .or_else(|| self.find_next(|server| connected_zone(server).is_some()));

    match idx {
      Some(idx) => {
        self.counter = idx;
        self.servers.get(idx)
      },
      None => self.next(),
    }
  }

  /// The index of the next server after the counter, in round-robin order, that matches `predicate`.
  fn find_next(&self, predicate: impl Fn(&Server) -> bool) -> Option<usize> {
    let len = self.servers.len();
    (1 ..= len)
      .map(|offset| (self.counter + offset) % len)
      .find(|&idx| predicate(&self.servers[idx]))
  }

  /// Conditionally add the server to the replica set.
  pub fn add(&mut self, server: Server) {
    if !self.servers.contains(&server) {
      self.servers.push(server);
    }
  }

  /// Remove the server from the replica set.
  pub fn remove(&mut self, server: &Server) {
    self.servers = self.servers.drain(..).filter(|_server| server != _server).collect();
  }

  /// The size of the replica set.
  pub fn len(&self) -> usize {
    self.servers.len()
  }

  /// Iterate over the replica set.
  pub fn iter(&self) -> impl Iterator<Item = &Server> {
    self.servers.iter()
  }
}

/// A container for round-robin routing to replica servers.
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ReplicaSet {
  /// A map of primary server IDs to a counter and set of replica server IDs.
  servers: HashMap<Server, ReplicaRouter>,
}

impl ReplicaSet {
  /// Create a new empty replica set.
  pub fn new() -> ReplicaSet {
    ReplicaSet {
      servers: HashMap::new(),
    }
  }

  /// Add a replica node to the routing table.
  pub fn add(&mut self, primary: Server, replica: Server) {
    self.servers.entry(primary).or_default().add(replica);
  }

  /// Remove a replica node mapping from the routing table.
  pub fn remove(&mut self, primary: &Server, replica: &Server) {
    let should_remove = if let Some(router) = self.servers.get_mut(primary) {
      router.remove(replica);
      router.len() == 0
    } else {
      false
    };

    if should_remove {
      self.servers.remove(primary);
    }
  }

  /// Remove the replica from all routing sets.
  pub fn remove_replica(&mut self, replica: &Server) {
    self.servers = self
      .servers
      .drain()
      .filter_map(|(primary, mut routing)| {
        routing.remove(replica);

        if routing.len() > 0 {
          Some((primary, routing))
        } else {
          None
        }
      })
      .collect();
  }

  /// Read the server ID of the next replica that should receive a command.
  pub fn next_replica(&mut self, primary: &Server) -> Option<&Server> {
    self.servers.get_mut(primary).and_then(|router| router.next())
  }

  /// Read all the replicas associated with the provided primary node.
  pub fn replicas(&self, primary: &Server) -> impl Iterator<Item = &Server> {
    self
      .servers
      .get(primary)
      .map(|router| router.iter())
      .into_iter()
      .flatten()
  }

  /// Return a map of replica nodes to primary nodes.
  pub fn to_map(&self) -> HashMap<Server, Server> {
    let mut out = HashMap::with_capacity(self.servers.len());
    for (primary, replicas) in self.servers.iter() {
      for replica in replicas.iter() {
        out.insert(replica.clone(), primary.clone());
      }
    }

    out
  }

  /// Clear the routing table.
  pub fn clear(&mut self) {
    self.servers.clear();
  }
}

/// A struct for routing commands to replica nodes.
#[cfg(feature = "replicas")]
pub struct Replicas {
  pub connections: HashMap<Server, Connection>,
  pub routing:     ReplicaSet,
  pub buffer:      VecDeque<Command>,
  pub read_from:   ReadFrom,
}

#[cfg(feature = "replicas")]
#[allow(dead_code)]
impl Replicas {
  pub fn new(read_from: ReadFrom) -> Replicas {
    Replicas {
      connections: HashMap::new(),
      routing: ReplicaSet::new(),
      buffer: VecDeque::new(),
      read_from,
    }
  }

  /// Find the replica connection that should serve the next command for the shard owned by `primary`.
  pub fn route(&mut self, primary: &Server) -> Option<&mut Connection> {
    let Replicas {
      connections,
      routing,
      read_from,
      ..
    } = self;

    let replica = match read_from.zone() {
      Some(zone) => routing
        .servers
        .get_mut(primary)?
        .next_in_zone(zone, |replica| connections.get(replica).map(|conn| conn.zone.as_ref())),
      None => routing.next_replica(primary),
    }?;
    connections.get_mut(replica)
  }

  /// Whether a replica of `primary` has a connection in `zone`.
  pub fn has_connection_in_zone(&self, primary: &Server, zone: &Str) -> bool {
    self.routing.replicas(primary).any(|replica| {
      self
        .connections
        .get(replica)
        .is_some_and(|conn| conn.zone.as_ref() == Some(zone))
    })
  }

  /// Sync the connection map in place based on the cached routing table.
  pub async fn sync_connections(&mut self, inner: &RefCount<ClientInner>) -> Result<(), Error> {
    for (_, mut writer) in self.connections.drain() {
      let commands = writer.close().await;
      self.buffer.extend(commands);
    }

    for (replica, primary) in self.routing.to_map() {
      self.add_connection(inner, primary, replica, false).await?;
    }

    Ok(())
  }

  /// Drop all connections and clear the cached routing table.
  pub async fn clear_connections(&mut self, inner: &RefCount<ClientInner>) -> Result<(), Error> {
    self.routing.clear();
    self.sync_connections(inner).await
  }

  /// Clear the cached routing table without dropping connections.
  pub fn clear_routing(&mut self) {
    self.routing.clear();
  }

  /// Connect to the replica and add it to the cached routing table.
  pub async fn add_connection(
    &mut self,
    inner: &RefCount<ClientInner>,
    primary: Server,
    replica: Server,
    force: bool,
  ) -> Result<(), Error> {
    _debug!(
      inner,
      "Adding replica connection {} (replica) -> {} (primary)",
      replica,
      primary
    );

    // A sync re-adds every replica. Opening a second connection to a connected replica would drop the first one
    // along with its in-flight commands.
    let connected = self.connections.contains_key(&replica);
    if !connected && (!inner.connection.replica.lazy() || force) {
      let mut transport = connection::create(inner, &replica, None).await?;
      transport.setup(inner, None).await?;

      if inner.config.server.is_clustered() {
        transport.readonly(inner, None).await?;
      };

      if let Some(id) = transport.id {
        inner
          .backchannel
          .connection_ids
          .lock()
          .insert(transport.server.clone(), id);
      }
      self.connections.insert(replica.clone(), transport.into_pipelined(true));
    }

    self.routing.add(primary, replica);
    Ok(())
  }

  /// Drop the socket associated with the provided server.
  pub async fn drop_writer(&mut self, inner: &RefCount<ClientInner>, replica: &Server) {
    if let Some(mut writer) = self.connections.remove(replica) {
      self.buffer.extend(writer.close().await);
      inner.backchannel.connection_ids.lock().remove(replica);
    }
  }

  /// Remove the replica from the routing table.
  pub fn remove_replica(&mut self, replica: &Server) {
    self.routing.remove_replica(replica);
  }

  /// Close the replica connection and optionally remove the replica from the routing table.
  pub async fn remove_connection(
    &mut self,
    inner: &RefCount<ClientInner>,
    primary: &Server,
    replica: &Server,
    keep_routable: bool,
  ) -> Result<(), Error> {
    _debug!(
      inner,
      "Removing replica connection {} (replica) -> {} (primary)",
      replica,
      primary
    );
    self.drop_writer(inner, replica).await;

    if !keep_routable {
      self.routing.remove(primary, replica);
    }
    Ok(())
  }

  /// Check and flush all the sockets managed by the replica routing state.
  pub async fn flush(&mut self) -> Result<(), Error> {
    for (_, writer) in self.connections.iter_mut() {
      writer.flush().await?;
    }

    Ok(())
  }

  /// Whether a working connection exists to any replica for the provided primary node.
  pub async fn has_replica_connection(&mut self, primary: &Server) -> bool {
    for replica in self.routing.replicas(primary) {
      if let Some(replica) = self.connections.get_mut(replica) {
        if replica.peek_reader_errors().await.is_some() {
          continue;
        } else {
          return true;
        }
      } else {
        continue;
      }
    }

    false
  }

  /// Return a map of `replica` -> `primary` server identifiers.
  pub fn routing_table(&self) -> HashMap<Server, Server> {
    self.routing.to_map()
  }

  /// Check the active connections and drop any without a working reader task.
  pub async fn drop_broken_connections(&mut self) {
    let mut new_writers = HashMap::with_capacity(self.connections.len());
    for (server, mut writer) in self.connections.drain() {
      if writer.peek_reader_errors().await.is_some() {
        self.buffer.extend(writer.close().await);
        self.routing.remove_replica(&server);
      } else {
        new_writers.insert(server, writer);
      }
    }

    self.connections = new_writers;
  }

  /// Read the set of all active connections.
  pub async fn active_connections(&mut self) -> Vec<Server> {
    join_all(self.connections.iter_mut().map(|(server, conn)| async move {
      if conn.peek_reader_errors().await.is_some() {
        None
      } else {
        Some(server.clone())
      }
    }))
    .await
    .into_iter()
    .flatten()
    .collect()
  }

  /// Take the commands stored for retry later.
  pub fn take_retry_buffer(&mut self) -> VecDeque<Command> {
    self.buffer.drain(..).collect()
  }

  pub async fn drain(&mut self, inner: &RefCount<ClientInner>) -> Result<(), Error> {
    // let inner = inner.clone();
    let _ = join_all(self.connections.iter_mut().map(|(_, conn)| conn.drain(inner)))
      .await
      .into_iter()
      .collect::<Result<Vec<()>, Error>>()?;

    Ok(())
  }
}

#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
pub fn map_replica_tls_names(inner: &RefCount<ClientInner>, primary: &Server, replica: &mut Server) {
  let policy = match inner.config.tls {
    Some(ref config) => &config.hostnames,
    None => {
      _trace!(inner, "Skip modifying TLS hostname for replicas.");
      return;
    },
  };
  if *policy == TlsHostMapping::None {
    _trace!(inner, "Skip modifying TLS hostnames for replicas.");
    return;
  }

  replica.set_tls_server_name(policy, &primary.host);
}

#[cfg(not(any(feature = "enable-native-tls", feature = "enable-rustls")))]
pub fn map_replica_tls_names(_: &RefCount<ClientInner>, _: &Server, _: &mut Server) {}

#[cfg(test)]
mod tests {
  use super::*;

  fn server(port: u16) -> Server {
    Server::new("127.0.0.1", port)
  }

  fn router(ports: &[u16]) -> ReplicaRouter {
    let mut router = ReplicaRouter::default();
    for port in ports {
      router.add(server(*port));
    }
    router
  }

  /// The zone of each connected replica, keyed by port.
  fn zones(zones: &[(u16, Option<&'static str>)]) -> HashMap<u16, Option<Str>> {
    zones
      .iter()
      .map(|(port, zone)| (*port, zone.map(Str::from_static)))
      .collect()
  }

  fn next_in_zone(router: &mut ReplicaRouter, zone: &str, connected: &HashMap<u16, Option<Str>>) -> Option<u16> {
    router
      .next_in_zone(&Str::from(zone), |replica| {
        connected.get(&replica.port).map(Option::as_ref)
      })
      .map(|replica| replica.port)
  }

  #[test]
  fn should_read_from_replica_in_zone() {
    let mut router = router(&[1, 2, 3]);
    let connected = zones(&[(1, Some("a")), (2, Some("b")), (3, Some("c"))]);

    for _ in 0 .. 5 {
      assert_eq!(next_in_zone(&mut router, "b", &connected), Some(2));
    }
  }

  #[test]
  fn should_round_robin_across_replicas_in_zone() {
    let mut router = router(&[1, 2, 3, 4]);
    let connected = zones(&[(1, Some("a")), (2, Some("b")), (3, Some("a")), (4, Some("b"))]);

    let ports: Vec<_> = (0 .. 4).map(|_| next_in_zone(&mut router, "a", &connected)).collect();
    assert_eq!(ports, vec![Some(3), Some(1), Some(3), Some(1)]);
  }

  #[test]
  fn should_fall_back_to_any_connected_replica_outside_zone() {
    let mut router = router(&[1, 2]);
    let connected = zones(&[(1, Some("b")), (2, None)]);

    let mut ports: Vec<_> = (0 .. 4).map(|_| next_in_zone(&mut router, "a", &connected)).collect();
    ports.sort();
    assert_eq!(ports, vec![Some(1), Some(1), Some(2), Some(2)]);
  }

  #[test]
  fn should_skip_disconnected_replica_in_zone() {
    let mut router = router(&[1, 2]);
    // the replica in zone "a" (port 1) lost its connection
    let connected = zones(&[(2, Some("b"))]);

    for _ in 0 .. 3 {
      assert_eq!(next_in_zone(&mut router, "a", &connected), Some(2));
    }
  }

  #[test]
  fn should_round_robin_when_no_replica_is_connected() {
    let mut router = router(&[1, 2]);
    let connected = zones(&[]);

    let ports: Vec<_> = (0 .. 4).map(|_| next_in_zone(&mut router, "a", &connected)).collect();
    assert_eq!(ports, vec![Some(2), Some(1), Some(2), Some(1)]);
  }

  #[test]
  fn should_connect_eagerly_when_zone_aware() {
    let config = ReplicaConfig::default();
    assert!(config.lazy());

    let config = ReplicaConfig {
      read_from: ReadFrom::AzAffinity("us-east4-a".into()),
      ..ReplicaConfig::default()
    };
    assert!(!config.lazy());
  }

  #[test]
  fn should_read_zone_from_read_from() {
    assert_eq!(ReadFrom::default(), ReadFrom::AnyReplica);
    assert_eq!(ReadFrom::AnyReplica.zone(), None);
    assert_eq!(
      ReadFrom::AzAffinityReplicasAndPrimary("us-east4-a".into()).zone(),
      Some(&Str::from_static("us-east4-a"))
    );
  }
}
