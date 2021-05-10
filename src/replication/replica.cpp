#include "replication/replica.h"

namespace noisepage::replication {

Replica::Replica(common::ManagedPointer<messenger::Messenger> messenger, const std::string &replica_name,
                 const std::string &hostname, uint16_t internal_port, uint16_t network_port)
    : replica_info_(messenger::ConnectionDestination::MakeTCP(replica_name, hostname, internal_port)),
      connection_id_(messenger->MakeConnection(replica_info_)),
      identity_(replica_name),
      hostname_(hostname),
      internal_port_(internal_port),
      network_port_(network_port) {}

}  // namespace noisepage::replication
