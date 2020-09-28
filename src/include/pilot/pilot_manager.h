#pragma once

#include <filesystem>
#include "messenger/messenger.h"
#include "loggers/messenger_logger.h"

namespace terrier::pilot {
class PilotLogic;
}

namespace terrier::pilot {
// TODO(ricky): read from some config file?
static constexpr const char *PILOT_ZMQ_PATH = "noisepage-pilot.ipc";
static constexpr const char *PILOT_CONN_ID_NAME = "pilot";
static constexpr const char *PILOT_TCP_HOST = "127.0.0.1";
static constexpr const int PILOT_TCP_PORT = 15645;

/**
 * Inteface for pilot related operations
 */
class PilotManager {
 public:
  PilotManager(std::string &&model_bin, const common::ManagedPointer<messenger::Messenger> &messenger);

  ~PilotManager() {
    StopPilot();
  }

  /**
   * Stop the model-pilot daemon
   */
  void StopPilot();

  pid_t GetModelPid() const { return py_pid_; }

 private:
  /**
   * This should be run as a thread routine.
   * 1. Make connection with the messenger
   * 2. Prepare arguments and forks to initialize a python daemon
   * 3. Record the pid
   */
  void StartPilot(std::string model_path, bool restart);

  std::string IPCPath() const { return (std::filesystem::current_path() / PILOT_ZMQ_PATH).string(); }
  std::string TCPPath() const { return fmt::format("{}:{}", PILOT_TCP_HOST, PILOT_TCP_PORT);}

  /** Messenger handler **/
  common::ManagedPointer<messenger::Messenger> messenger_;

  /** Connection **/
  messenger::ConnectionId conn_id_;

  /** Thread the pilot manager runs in **/
  std::thread thd_;

  /** Python model pid **/
  pid_t py_pid_ = -1;

  /** Bool shutting down **/
  bool shut_down_ = false;
};
}  // namespace terrier::pilot
