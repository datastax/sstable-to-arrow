import pyarrow as pa
from blazingsql import BlazingContext
import cudf
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
        sock.sendall(b'hello world')
        num_tables = read_u8(sock)
        table_buffers = []
        for i in range(num_tables):
            print('receiving table', i)
            table_size = read_u8(sock)
            buf = read_bytes(sock, table_size)
            table_buffers.append(buf)
    return table_buffers

buffers = fetch_data()
if len(buffers) != 1:
    print("This demo only works for a single IOT table. Remove this message if you are working with sstable-to-arrow on your own")
gdf = cudf.DataFrame.from_arrow(pa.ipc.open_stream(buffers[0]).read_all().flatten())

bc = BlazingContext()
bc.create_table("gpu_table", gdf)
bc.describe_table("gpu_table")
query = "SELECT * FROM gpu_table"
print('running query ' + query)
print(bc.explain(query))
result = bc.sql(query)
print(result)
