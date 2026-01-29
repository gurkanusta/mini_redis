import socket

HOST, PORT = "127.0.0.1", 6380

with socket.create_connection((HOST, PORT)) as s:
    print(s.recv(4096).decode())

    for cmd in ["PING", "SET a 1", "GET a", "EXPIRE a 5", "TTL a", "KEYS", "QUIT"]:
        s.sendall((cmd + "\n").encode("utf-8"))
        resp = s.recv(4096).decode("utf-8", errors="replace")
        print(cmd, "=>", resp)
