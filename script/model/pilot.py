#!/usr/bin/env python3
import zmq
import sys

end_point = sys.argv[1]
context = zmq.Context()

socket = context.socket(zmq.DEALER)
socket.setsockopt(zmq.IDENTITY, b"model")
# socket.connect(f"ipc://{end_point}")
socket.connect(f"tcp://{end_point}")
print(f"Python model connected at {end_point}")

# I am born
socket.send(b"", flags=zmq.SNDMORE)
socket.send(b"CConnected")

# Loop until quit
while(1):
    message = socket.recv()
    if message.decode("ascii") == "Quit":
        print("Asked to quit")
        socket.send(b"", flags=zmq.SNDMORE)
        socket.send(b"PQuit")
        break
    else:
        socket.send(b"", flags=zmq.SNDMORE)
        socket.send(b"PHeatbeat")



