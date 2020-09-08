#pragma once

#include <thread>  //NOLINT

namespace terrier::pilot {

/**
 * Inteface for pilot related operations
 */
class PilotManager {
 public:
  PilotManager()
  :thd_(std::thread([this] {
          StartPilot();
        }))
  {}

  /**
   * Stop the model-pilot daemon
   */
  void StopPilot() {
    thd_.join();
  }

 private:

  /**
   * This should be run as a thread routine.
   * 1. Initialize communication channels (pipe)
   * 2. Prepare arguments and forks to initialize a python daemon
   * 3. Record the pid
   */
  void StartPilot() {

  }
 std::thread thd_;
};

} // namespace terrier::pilot