#include "common/managed_pointer.h"
#include "gtest/gtest.h"
#include "network/connection_handle_factory.h"
#include "pilot/model_server.h"
#include "test_util/test_harness.h"

namespace terrier::pilot {

class ModelServerTest : public TerrierTest {
  void SetUp() override {
    network::network_logger->set_level(spdlog::level::info);
  }
};

// NOLINTNEXTLINE
TEST(ModelServerTest, ModelServerLifetimeTest) {
  LoggersUtil::Initialize();
  auto msg_logic = std::make_unique<messenger::MessengerLogic>();
  auto msg = std::make_unique<messenger::Messenger>(common::ManagedPointer(msg_logic));
  auto msg_thd = std::thread([&msg] {msg->RunTask();});

  auto model_manager =
      std::make_unique<pilot::ModelServerManager>("../../script/model/pilot.py", common::ManagedPointer(msg));
  std::this_thread::sleep_for (std::chrono::seconds(3)); /* make sure pilot has time to start */
  pid_t model_pid = model_manager->GetModelPid();
  EXPECT_NE(model_pid, -1);

  // Pilot Should be running
  EXPECT_EQ(kill(model_pid, 0), 0);

  // Kill the pilot should keep it running
  kill(model_pid, SIGKILL);
  std::this_thread::sleep_for (std::chrono::seconds(3));

  // No longer running
  EXPECT_EQ(kill(model_pid, 0), -1);

  std::this_thread::sleep_for (std::chrono::seconds(3));
  auto new_pid = model_manager->GetModelPid();
  EXPECT_GT(new_pid, 0);
  EXPECT_NE(new_pid, model_pid);

  // New pilot running
  EXPECT_EQ(kill(new_pid, 0), 0);

  model_manager->StopModelServer();
  std::this_thread::sleep_for (std::chrono::seconds(2));
  EXPECT_NE(kill(new_pid, 0), 0);

  msg->Terminate();
  msg_thd.join();
}

}  // namespace terrier::pilot
