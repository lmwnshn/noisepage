#!/usr/bin/env python3
import zmq
import sys

end_point = sys.argv[1]
context = zmq.Context()

socket = context.socket(zmq.DEALER)
socket.setsockopt(zmq.IDENTITY, b"model")
socket.connect(f"ipc://{end_point}")
print(f"Python model connected at {end_point}")

# I am born
socket.send(b"", flags=zmq.SNDMORE)
socket.send(b"CConnected")

import atexit
def cleanup_zmq():
    socket.close()
    context.destroy()
atexit.register(cleanup_zmq)

# Loop until quit
def runLoop():
    while(1):
        message = socket.recv()
        message = message.decode("ascii")
        if message == "Quit":
            print("Asked to quit")
            return
        elif message == "": # Empty delimiter
            continue
        else:
            print(message)
            socket.send(b"", flags=zmq.SNDMORE)
            socket.send(b"PHeatbeat")


runLoop()
print("Model shutting down")


