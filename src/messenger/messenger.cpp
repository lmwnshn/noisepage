#include "messenger/messenger.h"

#include <mutex>  // NOLINT
#include <vector>
#include <zmq.hpp>

#include "common/dedicated_thread_registry.h"
#include "common/error/exception.h"
#include "loggers/messenger_logger.h"
#include "messenger/connection_destination.h"

/*
 * A crash course on ZeroMQ (ZMQ).
 * To find out more about ZeroMQ, the best resource I've found is the official book: http://zguide.zeromq.org/
 *
 * # What is ZeroMQ?
 *
 *    To quote the ZMQ documentation,
 *      In the ZeroMQ universe, sockets are doorways to fast little background communications engines
 *      that manage a whole set of connections automagically for you.
 *
 * # Why use ZeroMQ?
 *
 *    The Messenger is meant to solve the following problem:
 *    1.  I have a message.
 *    2.  I want to send this message to another target. I don't care if this target is on the same process, on the
 *        same machine but a different process, or on a different machine over the network. The Messenger should
 *        figure it out and pick the best way of sending the message there.
 *
 *    This is exactly ZeroMQ's use case!
 *
 * # What benefits does ZeroMQ guarantee?
 *
 *    1.  ZeroMQ handles switching between tcp, ipc, and in-process communications very easily.
 *    2.  ZeroMQ performs IO asynchronously in the background. Alongside automatic message buffering,
 *        i.e., you can send() from a connected client before the server starts up, and the server will still get it!,
 *        this keeps Messenger from being bottlenecked on sending data over the network.
 *    3.  ZeroMQ automatically reconnects, as long as the dropped node comes back up eventually.
 *    4.  ZeroMQ has support for multipart message delivery.
 *    5.  ZeroMQ has atomic message delivery. This includes for multipart messages.
 *    6.  ZeroMQ has a very light message format that looks like (size | data), where data's format is up to you.
 *
 * # What pitfalls does ZeroMQ have?
 *
 *    1.  ZeroMQ is not truly zero-copy, copies are performed between userspace and kernelspace.
 *
 * # How does NoisePage use ZeroMQ?
 *
 *    ZeroMQ has established communication patterns that cover common use cases.
 *    These communication patterns are better described in the ZeroMQ book, linked above.
 *
 *    The Messenger does NOT use the REQUEST-REPLY pattern, but discussing it will provide useful context.
 *    REQUEST-REPLY (also known as REQ-REP) is the simplest pattern available in ZeroMQ.
 *    There are two types of sockets:
 *      1.  The REQUEST (client) socket, and
 *      2.  The REPLY (server) socket.
 *    All communication must be initiated by the REQUEST socket. In particular, all communication must look like:
 *      REQUEST.send() REPLY.recv() REPLY.send() REQUEST.recv() REQUEST.send() REPLY.recv() ...
 *    Otherwise, if send (or receive) is called twice in a row, an exception is thrown.
 *    This is clearly limiting. However, the reason this restriction exists is because the "one at a time" nature
 *    of send-recv-send-recv simplifies identity management, hiding it completely from the users of ZeroMQ.
 *    To remove the restriction, it is necessary for each message to contain some identity information.
 *
 *    This motivates the ROUTER-DEALER pattern.
 *        ROUTER = async servers.
 *        DEALER = async clients.
 *
 *    The Messenger uses a ROUTER-DEALER pattern.
 *    This is ZeroMQ terminology for saying that:
 *    1.  The server process exposes one main ROUTER socket.
 *    2.  The ROUTER socket is a "server" socket that asynchronously sends and receives messages to "clients".
 *        To help the ROUTER route messages, every message is expected to be multipart and of the following form:
 *        ROUTING_IDENTIFIER DELIMITER PAYLOAD
 *        where:
 *          - ROUTING_IDENTIFIER is controlled by setsockopt or getsockopt on ZMQ_ROUTING_ID.
 *          - DELIMITER is an empty message with size 0.
 *          - PAYLOAD is the message itself.
 *    3.  The DEALER socket must send messages in the same format as well.
 *
 * # Interfacing with the Messenger from Python.
 *
 *    This is an example of how you connect to the Messenger from Python.
 *      import zmq                                    # Use the ZeroMQ library.
 *      ctx = zmq.Context()                           # Create a ZeroMQ context for our entire process.
 *      sock = ctx.socket(zmq.DEALER)                 # We want an async DEALER socket for reasons described above.
 *      sock.setsockopt(zmq.IDENTITY, b'snek')        # Set the name of our Python program,
 *      sock.connect('ipc:///tmp/noisepage-ipc0')     # Connect to NoisePage on the same machine over IPC.
 *      sock.send(b'', flags=zmq.SNDMORE)             # Start building a message. This is the empty delimiter packet.
 *      s.send(b'PThis is the message payload.')      # Finish the message and send it. P is a prefix for Print.
 *
 */

namespace terrier::messenger {

/** An abstraction around ZeroMQ messages which explicitly have the sender specified. */
class ZmqMessage {
 public:
  /**
   * Build a new ZmqMessage from the supplied information.
   * @param send_msg_id     The ID of the message on the sender side.
   * @param recv_msg_id     The ID of the message on the receiver side.
   * @param sender_id       The identity of the sender.
   * @param message         The contents of the message.
   * @return A ZmqMessage encapsulating the given message.
   */
  static ZmqMessage Build(uint64_t send_msg_id, uint64_t recv_msg_id, const std::string &sender_id,
                          std::string_view message) {
    return ZmqMessage{sender_id, fmt::format("{}-{}-{}", send_msg_id, recv_msg_id, message)};
  }

  /**
   * Parse the given payload into a ZmqMessage.
   * @param routing_id      The message's routing ID.
   * @param message         The message received.
   * @return A ZmqMessage encapsulating the given message.
   */
  static ZmqMessage Parse(const std::string &routing_id, const std::string &message) {
    return ZmqMessage{routing_id, message};
  }

  /** @return The ID of this message (sender side). */
  uint64_t GetMessageIdSender() const { return send_msg_id_; }

  /** @return The ID of this message (receiver side). */
  uint64_t GetMessageIdReceiver() const { return recv_msg_id_; }

  /** @return The routing ID of this message. */
  std::string_view GetRoutingId() const { return std::string_view(routing_id_); }

  /** @return The message itself. */
  std::string_view GetMessage() const { return message_; }

  /** @return The raw payload of the message. */
  std::string_view GetRawPayload() const { return std::string_view(payload_); }

 private:
  friend Messenger;

  /** Construct a new ZmqMessage with the given routing ID and payload. Payload of form ID-MESSAGE. */
  ZmqMessage(std::string routing_id, std::string payload)
      : routing_id_(std::move(routing_id)), payload_(std::move(payload)), message_(payload_) {
    if (payload_.empty()) {
      send_msg_id_ = 0;
      recv_msg_id_ = 0;
    } else {
      // TODO(WAN): atoi, stoull, from_chars, etc? Error checking in general.
      UNUSED_ATTRIBUTE int check = std::sscanf(payload_.c_str(), "%lu-%lu-", &send_msg_id_, &recv_msg_id_);
      TERRIER_ASSERT(2 == check, "Couldn't parse the message header.");
      message_.remove_prefix(message_.find_last_of('-') + 1);
    }
  }

  /** The routing ID of the message. */
  std::string routing_id_;
  /** The payload in the message, of form ID-MESSAGE.  */
  std::string payload_;

  /** The cached id of the message (sender side). */
  uint64_t send_msg_id_;
  /** The cached id of the message (receiver side). */
  uint64_t recv_msg_id_;
  /** The cached actual message. */
  std::string_view message_;
};

/** An abstraction around all the ZeroMQ poll items that the Messenger holds. */
class MessengerPolledSockets {
 public:
  /** Wrapper around deficiency in C++ ZeroMQ poll API which doesn't have typed sockets. */
  class PollItems {
   public:
    /** The items to be polled. */
    std::vector<zmq::pollitem_t> items_;
  };

  /**
   * @return    The list of items to be polled on and their corresponding sockets.
   * @warning   Only one thread should be invoking GetPollItems().
   */
  PollItems &GetPollItems() {
    // If there are pending poll items, add all of them to the main list of poll items.
    if (!writer_.items_.empty()) {
      std::scoped_lock lock(mutex_);
      read_.items_.insert(read_.items_.cend(), writer_.items_.cbegin(), writer_.items_.cend());
      writer_.items_.clear();
    }
    return read_;
  }

  /** Include the specified @p socket on all subsequent calls to pollitems. */
  void AddPollItem(zmq::socket_t *socket) {
    zmq::pollitem_t pollitem;
    pollitem.socket = socket->operator void *();  // Poll on this raw socket.
    pollitem.events = ZMQ_POLLIN;                 // Event: at least one message can be received on the socket.
    {
      std::scoped_lock lock(mutex_);
      writer_.items_.emplace_back(pollitem);
    }
  }

 private:
  /** The items to be polled (reader side). */
  PollItems read_;
  /** The items to be polled that can't be added yet (writer side). */
  PollItems writer_;
  /** Protecting the transfer of poll items from writer side to reader side. */
  std::mutex mutex_;
};

}  // namespace terrier::messenger

namespace terrier {

/**
 * Useful ZeroMQ utility functions implemented in a naive manner. Most functions have wasteful copies.
 * If perf indicates that these functions are a bottleneck, switch to the zero-copy messages of ZeroMQ.
 */
class ZmqUtil {
 public:
  /** ZmqUtil is a static utility class that should not be instantiated. */
  ZmqUtil() = delete;

  /** The maximum length of a routing ID. Specified by ZeroMQ. */
  static constexpr int MAX_ROUTING_ID_LEN = 255;

  /** @return The routing ID of the socket. */
  static std::string GetRoutingId(common::ManagedPointer<zmq::socket_t> socket) {
    return socket->get(zmq::sockopt::routing_id);
  }

  /** @return The next string to be read off the socket. */
  static std::string Recv(common::ManagedPointer<zmq::socket_t> socket, zmq::recv_flags flags) {
    zmq::message_t message;
    auto received = socket->recv(message, flags);
    if (!received.has_value()) {
      throw MESSENGER_EXCEPTION(fmt::format("Unable to receive on socket: {}", ZmqUtil::GetRoutingId(socket)));
    }
    return std::string(static_cast<char *>(message.data()), received.value());
  }

  /** @return The next ZmqMessage (identity and payload) read off the socket. */
  static messenger::ZmqMessage RecvMsg(common::ManagedPointer<zmq::socket_t> socket) {
    std::string identity = Recv(socket, zmq::recv_flags::none);
    std::string delimiter = Recv(socket, zmq::recv_flags::none);
    std::string payload = Recv(socket, zmq::recv_flags::none);

    return messenger::ZmqMessage::Parse(identity, payload);
  }

  /** @return Send the specified identity over the socket. ROUTER sockets must send this before SendMsgPayload(). */
  static void SendMsgIdentity(common::ManagedPointer<zmq::socket_t> socket, const std::string &identity) {
    zmq::message_t identity_msg(identity.data(), identity.size());
    bool ok = socket->send(identity_msg, zmq::send_flags::sndmore).has_value();

    if (!ok) {
      throw MESSENGER_EXCEPTION(fmt::format("Unable to send on socket: {}", ZmqUtil::GetRoutingId(socket)));
    }
  }

  /** @return Send the specified ZmqMessage (delimiter and payload) over the socket. */
  static void SendMsgPayload(common::ManagedPointer<zmq::socket_t> socket, const messenger::ZmqMessage &msg) {
    zmq::message_t delimiter_msg("", 0);
    zmq::message_t payload_msg(msg.GetRawPayload().data(), msg.GetRawPayload().size());
    bool ok = true;

    ok = ok && socket->send(delimiter_msg, zmq::send_flags::sndmore).has_value();
    ok = ok && socket->send(payload_msg, zmq::send_flags::none).has_value();

    if (!ok) {
      throw MESSENGER_EXCEPTION(fmt::format("Unable to send on socket: {}", ZmqUtil::GetRoutingId(socket)));
    }
  }
};

}  // namespace terrier

namespace terrier::messenger {

ConnectionId::ConnectionId(common::ManagedPointer<Messenger> messenger, const ConnectionDestination &target,
                           const std::string &identity)
    : target_name_(target.GetTargetName()) {
  // Create a new DEALER socket and connect to the server.
  socket_ = std::make_unique<zmq::socket_t>(*messenger->zmq_ctx_, ZMQ_DEALER);
  socket_->set(zmq::sockopt::routing_id, identity);
  socket_->connect(target.GetDestination());
  routing_id_ = ZmqUtil::GetRoutingId(common::ManagedPointer(socket_));
  MESSENGER_LOG_TRACE(fmt::format("[PID={}] Connected to {} ({}) as {}.", ::getpid(), target_name_,
                                  target.GetDestination(), routing_id_.c_str()));
  // Add the new socket to the list of sockets that will be polled by the server loop.
  messenger->polled_sockets_->AddPollItem(socket_.get());
}

ConnectionId::~ConnectionId() = default;

ConnectionDestination Messenger::GetEndpointTCP(std::string target_name, const uint16_t port) {
  return ConnectionDestination::MakeTCP(std::move(target_name), MESSENGER_DEFAULT_TCP, port);
}

ConnectionDestination Messenger::GetEndpointIPC(std::string target_name, const uint16_t port) {
  return ConnectionDestination::MakeIPC(std::move(target_name), fmt::format(MESSENGER_DEFAULT_IPC, port));
}

ConnectionDestination Messenger::GetEndpointINPROC(std::string target_name, const uint16_t port) {
  return ConnectionDestination::MakeInProc(std::move(target_name), fmt::format(MESSENGER_DEFAULT_INPROC, port));
}

Messenger::Messenger(const uint16_t port, std::string identity) : port_(port), identity_(std::move(identity)) {
  // Create a ZMQ context. A ZMQ context abstracts away all of the in-process and networked sockets that ZMQ uses.
  // The ZMQ context is also the transport for in-process ("inproc") sockets.
  // Generally speaking, a single process should only have a single ZMQ context.
  // To have two ZMQ contexts is to have two separate ZMQ instances running, which is unlikely to be desired behavior.
  zmq_ctx_ = std::make_unique<zmq::context_t>();

  // Register a ROUTER socket on the default Messenger port.
  // A ROUTER socket is an async server process.
  zmq_default_socket_ = std::make_unique<zmq::socket_t>(*zmq_ctx_, ZMQ_ROUTER);
  // By default, the ROUTER socket silently discards messages that cannot be routed.
  // By setting ZMQ_ROUTER_MANDATORY, the ROUTER socket errors with EHOSTUNREACH instead.
  zmq_default_socket_->set(zmq::sockopt::router_mandatory, true);
  // Set the identity that this Messenger will be known by.
  zmq_default_socket_->set(zmq::sockopt::routing_id, identity_.c_str());

  // Bind the same ZeroMQ socket over the default TCP, IPC, and in-process channels.
  {
    ConnectionDestination dest_tcp = GetEndpointTCP("localhost", port);
    zmq_default_socket_->bind(dest_tcp.GetDestination());
    MESSENGER_LOG_INFO(fmt::format("[PID={}] Messenger listening: {}", ::getpid(), dest_tcp.GetDestination()));

    ConnectionDestination dest_ipc = GetEndpointIPC("localhost", port);
    zmq_default_socket_->bind(dest_ipc.GetDestination());
    MESSENGER_LOG_INFO(fmt::format("[PID={}] Messenger listening: {}", ::getpid(), dest_ipc.GetDestination()));

    ConnectionDestination dest_inproc = GetEndpointINPROC("localhost", port);
    zmq_default_socket_->bind(dest_inproc.GetDestination());
    MESSENGER_LOG_INFO(fmt::format("[PID={}] Messenger listening: {}", ::getpid(), dest_inproc.GetDestination()));
  }

  // The following block of code is dead code that contains useful background information on ZMQ defaults.
  {
    // ZeroMQ does all I/O in background threads. By default, a ZMQ context has one I/O thread.
    // An old version of the ZMQ guide suggests one I/O thread per GBps of data.
    // If I/O bottlenecks due to huge message volume, adjust this.
    // zmq_ctx_set(zmq_ctx_, ZMQ_IO_THREADS, NUM_IO_THREADS);
  }

  polled_sockets_ = std::make_unique<MessengerPolledSockets>();
  polled_sockets_->AddPollItem(zmq_default_socket_.get());
  is_messenger_running_ = true;
}

Messenger::~Messenger() = default;

void Messenger::RunTask() {
  try {
    // Run the server loop.
    ServerLoop();
  } catch (zmq::error_t &err) {
    Terminate();
  }
}

void Messenger::Terminate() {
  is_messenger_running_ = false;
  // Shut down the ZeroMQ context. This causes all existing sockets to abort with ETERM.
  zmq_ctx_->shutdown();
  MESSENGER_LOG_INFO(fmt::format("[PID={}] Messenger terminated.", ::getpid()));
}

void Messenger::ListenForConnection(const ConnectionDestination &target) {
  zmq_default_socket_->bind(target.GetDestination());
}

ConnectionId Messenger::MakeConnection(const ConnectionDestination &target) {
  return ConnectionId(common::ManagedPointer(this), target, identity_);
}

void Messenger::SendMessage(common::ManagedPointer<ConnectionId> connection_id, const std::string &message,
                            CallbackFn fn, uint64_t recv_msg_id) {
  uint64_t send_msg_id = message_id_++;
  // Check for wraparound.
  if (0 == message_id_) {
    message_id_ = static_cast<uint8_t>(BuiltinCallback::NUM_BUILTIN_CALLBACKS) + 1;
  }
  // Register the callback that will be invoked when a response to this message is received.
  callbacks_[send_msg_id] = fn;

  // Build and send the message.
  ZmqMessage msg = ZmqMessage::Build(send_msg_id, recv_msg_id, connection_id->routing_id_, message);
  ZmqUtil::SendMsgPayload(common::ManagedPointer(connection_id->socket_), msg);
  MESSENGER_LOG_TRACE(
      fmt::format("[PID={}] Messenger SENT-TO {}: {} ", ::getpid(), connection_id->target_name_, msg.GetRawPayload()));
}

void Messenger::ServerLoop() {
  while (is_messenger_running_) {
    // Get the latest set of poll items.
    auto poll_items = polled_sockets_->GetPollItems();
    // Poll on the current set of poll items.
    int num_sockets_with_data = zmq::poll(poll_items.items_, MESSENGER_POLL_TIMER);
    for (size_t i = 0; i < poll_items.items_.size(); ++i) {
      zmq::pollitem_t &item = poll_items.items_[i];
      // If no more sockets have data, then go back to polling.
      if (0 == num_sockets_with_data) {
        break;
      }
      // Otherwise, at least some socket has data. Is it the current socket?
      bool socket_has_data = item.revents & ZMQ_POLLIN;
      if (socket_has_data) {
        common::ManagedPointer<zmq::socket_t> socket(reinterpret_cast<zmq::socket_t *>(&item.socket));
        ZmqMessage msg = ZmqUtil::RecvMsg(socket);
        MESSENGER_LOG_TRACE("[PID={}] Messenger RECV-FR {}: {}", ::getpid(), msg.GetRoutingId(), msg.GetRawPayload());
        ProcessMessage(msg);
        --num_sockets_with_data;
      }
    }
  }
}

void Messenger::ProcessMessage(const ZmqMessage &msg) {
  auto recv_msg_id = msg.GetMessageIdReceiver();
  switch (recv_msg_id) {
    case static_cast<uint8_t>(BuiltinCallback::NOOP): {
      // Special function: NOOP.
      break;
    }
    case static_cast<uint8_t>(BuiltinCallback::ECHO): {
      // Special function: ECHO server.
      // ROUTER sockets must send their intended recipient as the first sndmore packet.
      // router_data must be the identity of a peer that connected directly to the router socket.
      MESSENGER_LOG_TRACE(fmt::format("Callback: echo {} {}", msg.GetRoutingId(), msg.GetRawPayload()));
      zmq::message_t router_data(msg.GetRoutingId().data(), msg.GetRoutingId().size());
      if (zmq_default_socket_->send(router_data, zmq::send_flags::sndmore).has_value()) {
        ZmqMessage reply = ZmqMessage::Build(0, msg.GetMessageIdSender(), identity_, msg.GetMessage());
        ZmqUtil::SendMsgIdentity(common::ManagedPointer(zmq_default_socket_.get()), identity_);
        ZmqUtil::SendMsgPayload(common::ManagedPointer(zmq_default_socket_.get()), reply);
      } else {
        throw MESSENGER_EXCEPTION("Failed to set router recipient.");
      }
      break;
    }
    default: {
      TERRIER_ASSERT(recv_msg_id > static_cast<uint8_t>(BuiltinCallback::NUM_BUILTIN_CALLBACKS), "Bad message ID.");
      // Default: there should be a stored callback.
      MESSENGER_LOG_TRACE(fmt::format("Callback: invoking stored callback {}", recv_msg_id));
      auto &callback = callbacks_.at(recv_msg_id);
      callback(msg.GetRoutingId(), msg.GetMessage());
      callbacks_.erase(recv_msg_id);
    }
  }
}

MessengerManager::MessengerManager(const common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry,
                                   const uint16_t port, const std::string &identity)
    : DedicatedThreadOwner(thread_registry),
      messenger_(thread_registry_->RegisterDedicatedThread<Messenger>(this, port, identity)) {}

}  // namespace terrier::messenger