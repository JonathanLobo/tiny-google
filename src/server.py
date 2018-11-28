import socket
import sys
import os
import struct
import time as tm
from queue import PriorityQueue

def send_msg(sock, msg):
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)

def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)

def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

def write(msg):
    return

def is_zero_file(fpath):
    return (not os.path.isfile(fpath)) or os.path.getsize(fpath) == 0

def main():
    my_port = 8000

    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the socket to the port
    my_address = socket.gethostbyname(socket.gethostname())
    server_address = (my_address, my_port)
    print('Starting up at address %s on port %s' % server_address)

    if my_port == 4202:
        exit()

    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(server_address)
        sock.listen(100)

        connection = None
        backlog = []

        while True:
            try:
                # Wait for a connection
                print('Waiting for a connection...')
                sock.settimeout(5)
                connection, client_address = sock.accept()
                print('Connection from' + str(client_address))
                # Receive the data
                data = recv_msg(connection)
                break
            except:
                print('Timed out')
                if (len(backlog) > 0):
                    print("Popped from backlog")
                    prev_session = backlog.pop(0)
                    data = prev_session[0]
                    client_address = prev_session[1]
                    break

        if data:
            data_parsed = data.decode("utf-8").split(',')
            message = data_parsed[0]
            ts = int(data_parsed[1])
            print('Received ' + message)
            time = max(time, ts) + 1
            if (message == 'READ'):
                if (not waiting_for_cs and not waiting_for_read):
                    # a client wants to read the last value written to shared text file
                    time = time + 1
                    out_ts = time

                    wr_address.append(client_address[0])
                    wr_port.append(int(data_parsed[3]))

                    server_list = []
                    port_list = []
                    with open('../server_list.txt', 'r') as myfile:
                        for line in myfile:
                            print(line)
                            parsed = line.split(':')
                            server_list.append(parsed[0].strip('\n'))
                            port_list.append(parsed[1].strip('\n'))
                    num_servers = len(server_list)

                    # ask permission from all servers to enter CS
                    for i in range(len(server_list)):
                        if not(server_list[i] == my_address and int(port_list[i]) == server_port):
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            server_address = (server_list[i], int(port_list[i]))
                            replyString = 'REQUEST, ' + str(out_ts) + ', 0, ' + str(server_port)
                            # print(server_address)
                            while(True):
                                try:
                                    sock.connect(server_address)
                                    send_msg(sock, replyString.encode())
                                    break
                                except: pass

                    waiting_for_read = True
                    waiting_ts = ts
                else:
                    backlog.append((data, client_address))

    # Clean up the connection
    connection.close()

main()
