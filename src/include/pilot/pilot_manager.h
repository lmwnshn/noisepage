#pragma once

#include "messenger/messenger.h"

namespace terrier::pilot {
// TODO(ricky): read from some config file?
static constexpr const char *PILOT_ZMQ_PATH = "noisepage-pilot";
static constexpr const char *PILOT_CONN_NAME = "pilot";

/**
 * Inteface for pilot related operations
 */
class PilotManager {
 public:
  PilotManager(std::string model_bin, common::ManagedPointer<messenger::Messenger> messenger);

  /**
   * Stop the model-pilot daemon
   */
  void StopPilot();

 private:
  /**
   * This should be run as a thread routine.
   * 1. Initialize communication channels (pipe)
   * 2. Prepare arguments and forks to initialize a python daemon
   * 3. Record the pid
   * 4. Call the Messenger to initialize a connection
   */
  void StartPilot(std::string model_path, bool restart);

  /* Messenger handler */
  common::ManagedPointer<messenger::Messenger> messenger_;

  /* Connection */
  messenger::ConnectionId conn_id_;

  /* Thread the pilot manager runs in */
  std::thread thd_;

  /* Python model pid */
  pid_t py_pid_;

  /* Bool shutting down */
  bool shut_down_ = false;
};

}  // namespace terrier::pilot
