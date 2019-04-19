import socket

ip_adr=('127.0.0.1', 8900)
sk = socket.socket()
sk.connect(ip_adr)
sk.sendall(bytes('hello world'))
reply = sk.recv(1024)
print str(reply)
sk.close()
