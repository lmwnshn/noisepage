#pragma once

#include <filesystem>
#include "messenger/messenger.h"
#include "loggers/messenger_logger.h"

namespace terrier::pilot {
class PilotLogic;
}

namespace terrier::pilot {
// TODO(ricky): read from some config file?
static constexpr const char *MODEL_CONN_ID_NAME = "model";
static constexpr const char *MODEL_ZMQ_PATH = "/tmp/noisepage-ipc0";
static constexpr const char *MODEL_TCP_HOST = "127.0.0.1";
static constexpr const int MODEL_TCP_PORT = 15645;

/**
 * Inteface for pilot related operations
 */
class ModelServerManager {
 public:
  ModelServerManager(std::string &&model_bin, const common::ManagedPointer<messenger::Messenger> &messenger);

  ~ModelServerManager() { StopModelServer();
  }

  /**
   * Stop the model-pilot daemon
   */
  void StopModelServer();

  pid_t GetModelPid() const { return py_pid_; }

 private:
  /**
   * This should be run as a thread routine.
   * 1. Make connection with the messenger
   * 2. Prepare arguments and forks to initialize a python daemon
   * 3. Record the pid
   */
  void StartModelServer(std::string model_path);

  std::string IPCPath() const { return MODEL_ZMQ_PATH; }
  std::string TCPPath() const { return fmt::format("{}:{}", MODEL_TCP_HOST, MODEL_TCP_PORT);}

  /** Messenger handler **/
  common::ManagedPointer<messenger::Messenger> messenger_;

  /** Connection **/
  //messenger::ConnectionId conn_id_;

  /** Thread the pilot manager runs in **/
  std::thread thd_;

  /** Python model pid **/
  pid_t py_pid_ = -1;

  /** Bool shutting down **/
  bool shut_down_ = false;
};
}  // namespace terrier::pilot
