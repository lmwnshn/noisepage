#pragma once

#include "messenger/messenger.h"

namespace  terrier::pilot{
class PilotLogic;
}

namespace terrier::pilot {
// TODO(ricky): read from some config file?
static constexpr const char *PILOT_ZMQ_PATH = "/tmp/noisepage-pilot";
static constexpr const char *PILOT_CONN_ID_NAME = "pilot";


/**
 * Inteface for pilot related operations
 */
class PilotManager {
 public:
  PilotManager(std::string && model_bin, const common::ManagedPointer<messenger::Messenger> &messenger);

  /**
   * Stop the model-pilot daemon
   */
  void StopPilot();



 private:
  /**
   * This should be run as a thread routine.
   * 1. Make connection with the messenger
   * 2. Prepare arguments and forks to initialize a python daemon
   * 3. Record the pid
   */
  void StartPilot(std::string model_path, bool restart);

  /** Messenger handler **/
  common::ManagedPointer<messenger::Messenger> messenger_;

  /** Logic for messenger **/
  std::unique_ptr<PilotLogic> logic_;

  /** Connection **/
  messenger::ConnectionId conn_id_;

  /** Thread the pilot manager runs in **/
  std::thread thd_;

  /** Python model pid **/
  pid_t py_pid_;

  /** Bool shutting down **/
  bool shut_down_ = false;
};

/**
 *
 */
class PilotLogic : messenger::MessengerLogic {
  enum Callbacks: uint8_t {NOOP = 'N', PRINT='P'};

  void ProcessMessage(std::string_view sender, std::string_view message);
};

}  // namespace terrier::pilot
