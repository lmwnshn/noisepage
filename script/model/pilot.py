#!/usr/bin/env python3
import zmq

context = zmq.Context()

#  Socket to talk to server
print("Connecting to hello world server…")
socket = context.socket(zmq.DEALER)
socket.setsockopt(zmq.IDENTITY, b'model')
socket.connect("ipc:///tmp/noisepage-pilot")



# Loop until quit
while(1):
    socket.send(b'PPing')
    print(f'Send [Ping]')

    message = socket.recv()
    print(f"Received reply [ {message} ]")


