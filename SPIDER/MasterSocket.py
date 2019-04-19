import socket

ip_adr = ('127.0.0.1', 8900)

web = socket.socket()
web.bind(ip_adr)
web.listen(6)

while True:
    conn, adr = web.accept()
    data = conn.recv(1024)
    print data
    conn.send(bytes('<h1>welcome nginx</h1>'))
    conn.close()
