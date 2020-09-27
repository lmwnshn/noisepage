#include "pilot/pilot_manager.h"

#include <sys/wait.h>

#include <thread>  // NOLINT

#include "loggers/network_logger.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger.h"

namespace terrier::pilot {

/** @return The pilot's IPC connection for the Python process to talk to. */
messenger::ConnectionId ListenAndMakeConnection(common::ManagedPointer<messenger::Messenger> messenger) {
  // Create an IPC connection that the Python process will talk to.
  auto destination = messenger::ConnectionDestination::MakeIPC(PILOT_ZMQ_PATH);
  // Start listening over IPC.
  messenger->ListenForConnection(destination);
  return messenger->MakeConnection(destination, PILOT_CONN_NAME);
}

}  // namespace terrier::pilot

namespace terrier::pilot {

PilotManager::PilotManager(std::string model_bin, common::ManagedPointer<messenger::Messenger> messenger)
    : messenger_(messenger),
      conn_id_(ListenAndMakeConnection(messenger_)),
      thd_(std::thread([this, model_bin] { StartPilot(model_bin, false); })) {}

void PilotManager::StartPilot(std::string model_path, bool restart) {
  py_pid_ = ::fork();
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
    wait_pid = ::waitpid(py_pid_, &status, 0);

    if (wait_pid < 0) {
      NETWORK_LOG_ERROR("Failed to wait for the child process...");
      return;
    }

    // Restart the pilot if the main database is still running
    if (!shut_down_) {
      StartPilot(model_path, false);
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

void PilotManager::StopPilot() {
  shut_down_ = true;
  kill(py_pid_, SIGTERM);
}

}  // namespace terrier::pilot