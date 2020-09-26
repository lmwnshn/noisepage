#pragma once

#include <thread>  //NOLINT

#include "loggers/network_logger.h"
#include "messenger/messenger.h"

namespace terrier::pilot {
// TODO(ricky): read from some config file?
static constexpr const char *PILOT_ZMQ_PATH = "tcp://noisepage-pilot";

/**
 * Inteface for pilot related operations
 */
class PilotManager {
 public:
  PilotManager(std::string model_bin, common::ManagedPointer<messenger::Messenger> messenger)
      : messenger_(messenger),
        conn_id_(
            messenger_->MakeConnection(messenger::ConnectionDestination(PILOT_ZMQ_PATH), std::nullopt)),
        thd_(std::thread([this, model_bin] { StartPilot(model_bin); })) {}

  /**
   * Stop the model-pilot daemon
   */
  void StopPilot() {
    shut_down_ = true;
    kill(py_pid_, SIGTERM);
  }

 private:
  /**
   * This should be run as a thread routine.
   * 1. Initialize communication channels (pipe)
   * 2. Prepare arguments and forks to initialize a python daemon
   * 3. Record the pid
   * 4. Call the Messenger to initialize a connection
   */
  void StartPilot(std::string model_path, bool restart = false) {
    py_pid_ = fork();
    if (py_pid_ < 0) {
      NETWORK_LOG_ERROR("Failed to fork to spawn model process");
      return;
    }

    // Fork success
    if (py_pid_ > 0) {
      // Parent Process Routine
      NETWORK_LOG_INFO("Pilot Process running at : {}", py_pid_);

      // Wait for the child to exit
      int status;
      pid_t wait_pid;

      // Wait for the child
      wait_pid = waitpid(py_pid_, &status, 0);

      if (wait_pid < 0) {
        NETWORK_LOG_ERROR("Failed to wait for the child process...");
        return;
      }

      // Restart the pilot if the main database is still running
      if (!shut_down_) {
        StartPilot(model_path);
      }
    } else {
      // Run the script in in a child
      std::string python3_bin("/usr/local/bin/python3");
      char *args[] = {python3_bin.data(), model_path.data(), nullptr};
      if (execvp(args[0], args) < 0) {
        NETWORK_LOG_ERROR("Failed to execute model binary: {}", strerror(errno));
      }
    }
  }
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
