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
    std::string msg = "Hello Model!";
    struct sockaddr_in server;
    struct hostent *hp;

    /* Create socket */
    sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_ < 0) {
      perror("opening stream socket");
      exit(1);
    }

    /* Connect socket using name specified by command line.  */
    server.sin_family = AF_INET;
    hp = gethostbyname(host_.c_str());
    if (hp == 0) {
      NETWORK_LOG_ERROR("Failed to get host at {}", host_);
      return;
    }
    memcpy(&server.sin_addr, hp->h_addr, hp->h_length);
    server.sin_port = htons(port_);
    if (connect(sock_, (struct sockaddr *)&server, sizeof(server)) < 0) {
      NETWORK_LOG_ERROR("connecting stream socket failed: {}", strerror(errno));
      return;
    }

    SendMsgToModel(msg);
  }

  void SendMsgToModel(const std::string &msg) {
    if (write(sock_, msg.c_str(), sizeof(msg.size())) < 0) {
      NETWORK_LOG_ERROR("Failed to write to the model");
    }
  }

 std::thread thd_;
 std::string host_ = "localhost";
 int sock_ = -1;
 int port_ = 15445;
};

} // namespace terrier::pilot