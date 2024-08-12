import json
import socket
from connection_pool.server_package.config import server_data


class Server:
    def __init__(self, srv_host, srv_port, srv_buff):
        self.srv_host = srv_host
        self.srv_port = int(srv_port)
        self.srv_buff = int(srv_buff)

    def server_connection(self):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.srv_host, self.srv_port))
            s.listen()
            print("Server started.")
            while True:
                conn, addr = s.accept()
                with conn:
                    print(f"Connected by {addr}")
                    received_data = conn.recv(self.srv_buff)
                    print(f'Server USER DATA = {received_data}')

    @staticmethod
    def json_decode_received_data(received_data):
        decoded_data = json.loads(received_data)
        if 'login' in decoded_data['command']:
            print(f"Command received from Client: login")
            return decoded_data["command"]
        else:
            print(f"Command received from Client: {decoded_data['command']}")
        return decoded_data["command"]

    @staticmethod
    def json_serialize_response(response):
        return json.dumps(response)


def start():
    params = server_data()
    server = Server(params['host'], params['port'], params['buffer_size'])
    server.server_connection()


if __name__ == '__main__':
    start()
