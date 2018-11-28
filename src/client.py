import socket
import sys
import struct
import math
import itertools

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


def main():
    client_port = 7000
    server_port = 8000
    server_ip = '192.168.1.153'

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect the socket to the port where the assigned server is listening
    server_address = (server_ip, server_port)
    print('Connecting to ' + server_address[0])
    connection = sock.connect(server_address)


    # message = 'READ, ' + str(ts) + ', ' + str(client_port)
    # send_msg(sock,message.encode())

    sock.close()

    # if (writer):
    #     while len(nums) > 0: # WHILE HAS NUMBERS LEFT TO SUM
    #
    #         # Create a TCP/IP socket
    #         sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #         sock.settimeout(1)
    #
    #         time = time + 1
    #         ts = time
    #         print("TIME: " + str(ts))
    #         next_int = nums.pop(0)
    #         message = 'WRITE, ' + str(ts) + ', ' + str(next_int) + ', ' + str(client_port)
    #
    #         # Connect the socket to the port where the assigned server is listening
    #         server_address = (ip, port)
    #         # print('Connecting to %s port %s' % server_address)
    #         while(True):
    #             try:
    #                 print('Connecting to ' + server_address[0])
    #                 sock.connect(server_address)
    #                 send_msg(sock,message.encode())
    #                 break
    #             except Exception as e:
    #                 print(e)
    #                 pass
    #
    #         # sock.close()
    #
    #         print("Sent a write...")
    #
    #         # create new socket to listen for reply
    #         sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #         # Bind the socket to the port
    #         server_address = (socket.gethostbyname(socket.gethostname()), client_port)
    #         sock.bind(server_address)
    #         sock.listen(1)
    #
    #         connection = None
    #         try:
    #             # Wait for a connection
    #             print('Waiting for a reply...')
    #             connection, client_address = sock.accept()
    #         except KeyboardInterrupt:
    #             if connection:
    #                 connection.close()
    #             break
    #
    #         print('Connection from' + str(client_address))
    #         data = recv_msg(connection).decode()
    #         parsed = data.split(',')
    #         message = parsed[0]
    #         ts = int(parsed[1])
    #         time = max(time, ts) + 1
    #         print("Received " + message)
    #         # Theoretically, we should be checking what this message says
    #         # However, the server will ONLY reply if worked so we don't really care
    #         sock.close()
    # else:
    #     for i in range(num_reads):
    #         # Create a TCP/IP socket
    #
    #
    #         # create new socket to listen for reply
    #         sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #         # Bind the socket to the port
    #         server_address = (socket.gethostbyname(socket.gethostname()), client_port)
    #         sock.bind(server_address)
    #         sock.listen(1)
    #
    #         data = recv_msg(sock).decode()
    #         parsed = data.decode("utf-8").split(',')[0]
    #         message = parsed[0]
    #         ts = parsed[1]
    #         print('Message: ' + str(message) + ' TS: ' + str(ts))
    #         time = max(time, ts) + 1
    #         sock.close()


main()
