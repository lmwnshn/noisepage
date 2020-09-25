#pragma once


#include <thread>  //NOLINT
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <netinet/in.h>

#include "loggers/network_logger.h"

namespace terrier::pilot {

/**
 * Inteface for pilot related operations
 */
class PilotManager {
 public:
  PilotManager(std::string model_bin)
  :thd_(std::thread([this, model_bin] {
          StartPilot(model_bin);
        }))
  {}

  /**
   * Stop the model-pilot daemon
   */
  void StopPilot() {
    shut_down_ = true;
    kill(process_pid_,SIGTERM);
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
    // Set up the pipes
    if(pipe(fd_to_model_) < 0) {
      NETWORK_LOG_ERROR("Failed to initialize pipes to model");
      return;
    }

    if(pipe(fd_from_model_) < 0) {
      NETWORK_LOG_ERROR("Failed to initialize pipes from model");
      close(fd_to_model_[0]);
      close(fd_to_model_[1]);
      return;
    }

    process_pid_ = fork();
    if(process_pid_ < 0) {
      NETWORK_LOG_ERROR("Failed to fork to spawn model process");
      ClosePipes();
      return;
    }

    // Fork success
    if(process_pid_ > 0) {
      // Parent Process Routine
      NETWORK_LOG_INFO("Pilot Process running at : {}", process_pid_);
      // Set up pipes
      close(fd_from_model_[1]);
      close(fd_to_model_[0]);

      // Initialize connection through Messenger
      //con_id_ = msg_connector_->MakeConnection(fd_to_model_[1], fd_from_model_[0], process_pid_);

      // TODO(ricky):
      //  The pilot manager is responsible for setting up handlers/callbacks for
      //  all message types here

      // Wait for the child to exit
      int status;
      pid_t wait_pid;

      // FIXME(ricky): Recv should be done at the Messenger
      char buf[100] = {0};
      read(fd_from_model_[0], buf, 100);
      NETWORK_LOG_INFO("RECV: {}", buf);
      wait_pid = waitpid(process_pid_, &status, 0);

      if(wait_pid < 0) {
        NETWORK_LOG_ERROR("Failed to wait for the child process...");
        ClosePipes();
        // TODO(ricky): Notify the messenger?
        return;
      }


      // The child process terminated
      // TODO(ricky): Do we care why???

      // Restart the pilot if the main database is still running
      if(!shut_down_) {
        StartPilot(model_path, true);
      }
    }
    else {

      std::string python3_bin("/usr/local/bin/python3");
      //std::string python3_bin("cat");
      char *args[] = { python3_bin.data(), model_path.data(), nullptr};

      dup2(fd_to_model_[0], STDIN_FILENO);
      dup2(fd_from_model_[1], STDOUT_FILENO);

      close(fd_to_model_[0]);
      close(fd_from_model_[1]);

      if(execvp(args[0], args) < 0) {
        NETWORK_LOG_ERROR("Failed to execute model binary: {}", strerror(errno));
      }
    }
  }

 std::thread thd_;

 pid_t process_pid_;
 bool shut_down_ = false;

 // FIXME(ricky):
 //common::ManagedPointer<network::Messenger> msg_connector_;
};

} // namespace terrier::pilot
