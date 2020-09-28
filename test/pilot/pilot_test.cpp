#include "pilot/pilot_manager.h"

#include "common/managed_pointer.h"
#include "gtest/gtest.h"
#include "network/connection_handle_factory.h"
#include "test_util/test_harness.h"

namespace terrier::pilot {

class PilotTest : public TerrierTest {
  void SetUp() override {
    network::network_logger->set_level(spdlog::level::info);
  }
};

// NOLINTNEXTLINE
TEST(PilotTest, PilotLifeTimeTest) {
  LoggersUtil::Initialize();
  auto msg_logic = std::make_unique<messenger::MessengerLogic>();
  auto msg = std::make_unique<messenger::Messenger>(common::ManagedPointer(msg_logic));
  auto msg_thd = std::thread([&msg] {msg->RunTask();});

  auto pilot_manager =
      std::make_unique<pilot::PilotManager>("../../script/model/pilot.py", common::ManagedPointer(msg));
  std::this_thread::sleep_for (std::chrono::seconds(3)); /* make sure pilot has time to start */
  pid_t model_pid = pilot_manager->GetModelPid();
  EXPECT_NE(model_pid, -1);

  // Pilot Should be running
  EXPECT_EQ(kill(model_pid, 0), 0);

  pilot_manager->StopPilot();
  std::this_thread::sleep_for (std::chrono::seconds(2));
  EXPECT_NE(kill(model_pid, 0), 0);

  // // Kill the pilot should keep it running
  // kill(model_pid, SIGKILL);
  // std::this_thread::sleep_for (std::chrono::seconds(3));

  // // No longer running
  // EXPECT_EQ(kill(model_pid, 0), -1);

  // std::this_thread::sleep_for (std::chrono::seconds(3));
  // auto new_pid = pilot_manager->GetModelPid();
  // EXPECT_GT(new_pid, 0);
  // EXPECT_NE(new_pid, model_pid);

  // // New pilot running
  // EXPECT_EQ(kill(new_pid, 0), 0);

  // pilot_manager->StopPilot();
  // std::this_thread::sleep_for (std::chrono::seconds(2));
  // EXPECT_NE(kill(new_pid, 0), 0);
}

}  // namespace terrier::pilot
