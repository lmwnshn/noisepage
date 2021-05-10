#pragma once

#include <string>

#include "common/managed_pointer.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger.h"

namespace noisepage::replication {

/** Abstraction around a replica. */
class Replica {
 public:
  /**
   * Create a replica.
   *
   * @param messenger       The messenger to use.
   * @param replica_name    The name of the replica.
   * @param hostname        The hostname of the replica.
   * @param internal_port   The internal port of the replica used for replication.
   * @param network_port    The network port of the replica used for connecting over psql.
   */
  Replica(common::ManagedPointer<messenger::Messenger> messenger, const std::string &replica_name,
          const std::string &hostname, uint16_t internal_port, uint16_t network_port);

  /** @return The connection ID for this replica. */
  messenger::connection_id_t GetConnectionId() const { return connection_id_; }

  /** @return The identity of the replica on the replication network. */
  const std::string &GetIdentity() const { return identity_; }
  /** @return The hostname of the replica. */
  const std::string &GetHostname() const { return hostname_; }
  /** @return The internal port of the replica used for replication. */
  uint16_t GetInternalPort() const { return internal_port_; }
  /** @return The network port of the replica used for connecting over psql. */
  uint16_t GetNetworkPort() const { return network_port_; }

 private:
  messenger::ConnectionDestination replica_info_;  ///< The connection metadata for this replica.
  messenger::connection_id_t connection_id_;       ///< The connection ID to this replica.
  std::string identity_;                           ///< The identity of the replica.
  std::string hostname_;                           ///< The hostname of the replica.
  uint16_t internal_port_;                         ///< The internal port of the replica used for replication.
  uint16_t network_port_;                          ///< The network port of the replica used for psql.
};

}  // namespace noisepage::replication
