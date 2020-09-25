#!/usr/bin/env python3
import os
import sys
import time

sys.stdout.write(f"\nPilot Model at {os.getpid()}!\n")
sys.stdout.write("Hello world!\n")
time.sleep(5)

import socket

HOST = "localhost"
PORT = 15445

with socket.socket(socket.AF_NET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    print("Connected...")
    data = s.recv(1024)

    print(f"Received: \n %{data}")


