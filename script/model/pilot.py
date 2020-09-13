#!/usr/bin/env python3

import socket

HOST = "localhost"
PORT = 15445

with socket.socket(socket.AF_NET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    print("Connected...")
    data = s.recv(1024)

    print(f"Received: \n %{data}")




