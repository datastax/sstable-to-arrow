import pyarrow as pa
import pandas as pd
import socket

HOST = '127.0.0.1'
PORT = 9143

def read_bytes(sock, n):
    data = b''
    while len(data) < n:
        more = sock.recv(n - len(data))
        if not more:
            raise EOFError("Socket connection ended before reading specified number of bytes")
        data += more
    return data

def read_u8(sock):
    data = read_bytes(sock, 8)
    return int.from_bytes(data, byteorder='big')

# read data from socket
def fetch_data():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        sock.sendall(b'hello world\n')
        num_tables = read_u8(sock)
        table_buffers = []
        for i in range(num_tables):
            print('receiving table', i)
            table_size = read_u8(sock)
            buf = read_bytes(sock, table_size)
            table_buffers.append(buf)
    return table_buffers

buffers = fetch_data()
tables = [pa.ipc.open_stream(buf).read_all() for buf in buffers]
for i, table in enumerate(tables):
    print(f'TABLE {i}' table.to_pandas())
