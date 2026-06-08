import threading
import random
import time
import pickle
import sys
from socket import *
from requests import get

import grpc
import name_service_pb2
import name_service_pb2_grpc
from constMP import NAME_SERVICE_ADDR, NAME_SERVICE_PORT, N

class PeerNode:
    def __init__(self, peer_name, udp_port, tcp_port):
        self.my_name = peer_name
        self.my_id = None
        self.num_msgs = 0
        self.peers = [] # Armazenará tuplas de (ip, porta_udp) obtidas via gRPC
        self.handshake_count = 0
        self.lock = threading.Lock()
        
        self.udp_port = udp_port
        self.tcp_port = tcp_port

        # Configuração de Sockets
        self.send_socket = socket(AF_INET, SOCK_DGRAM)
        self.recv_socket = socket(AF_INET, SOCK_DGRAM)
        self.recv_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.recv_socket.bind(('0.0.0.0', self.udp_port))

        self.server_sock = socket(AF_INET, SOCK_STREAM)
        self.server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server_sock.bind(('0.0.0.0', self.tcp_port))
        self.server_sock.listen(1)

        # Canal gRPC com o Name Service
        self.channel = grpc.insecure_channel(f'{NAME_SERVICE_ADDR}:{NAME_SERVICE_PORT}')
        self.ns_stub = name_service_pb2_grpc.NameDirectoryServiceStub(self.channel)

    def get_public_ip(self):
        try:
            return get('https://api.ipify.org').content.decode('utf8')
        except Exception:
            return '127.0.0.1'

    def register_and_advertise(self):
        ip_addr = self.get_public_ip()
        
        # 1. Bind do endereço TCP (Usado para o Comparison Server achá-lo)
        # Nota: Como o peer usa duas portas (UDP e TCP), salvamos a porta TCP no lookup principal
        # e passamos a UDP de forma flexível ou assumida.
        bind_req = name_service_pb2.BindRequest(
            name=self.my_name,
            address=name_service_pb2.Address(ip=ip_addr, port=self.tcp_port)
        )
        res1 = self.ns_stub.Bind(bind_req)
        
        # 2. Registra o atributo do tipo "peer" no serviço de diretório
        if res1.success:
            reg_req = name_service_pb2.RegisterTypeRequest(name=self.my_name, type="peer")
            self.ns_stub.RegisterType(reg_req)
            print(f"[{self.my_name}] Registrado com sucesso no gRPC Name Service.")
        else:
            print(f"Erro ao registrar: {res1.error_message}")

    def update_peer_list_via_directory(self):
        """Substitui completamente a função do antigo Group Manager"""
        disc_req = name_service_pb2.DiscoverRequest(type="peer")
        response = self.ns_stub.Discover(disc_req)
        
        self.peers = []
        for proc in response.processes:
            # Ignora a si mesmo na lista de envio de broadcasts
            if proc.name != self.my_name:
                # IMPORTANTE: No nosso cenário UDP simplificado, assumimos a porta padrão do peer remoto 
                # ou calculada de forma previsível (Ex: porta_tcp + 1110) se rodando localmente.
                # Para simplificar na mesma máquina, mapeamos uma lógica de portas UDP correspondentes.
                remote_udp = proc.address.port + 1110 
                self.peers.append((proc.address.ip, remote_udp))
        print(f"[{self.my_name}] Lista de peers atualizada via Diretório: {self.peers}")

    def wait_for_start_signal(self):
        print(f'[{self.my_name}] Aguardando sinal TCP de início na porta {self.tcp_port}...')
        conn, addr = self.server_sock.accept()
        msg_pack = conn.recv(1024)
        msg = pickle.loads(msg_pack)
        
        self.my_id = msg[0]
        self.num_msgs = msg[1]
        
        response = f'Peer {self.my_name} (ID: {self.my_id}) iniciado.'
        conn.send(pickle.dumps(response))
        conn.close()
        return self.my_id, self.num_msgs

    def broadcast_handshake(self):
        for ip, udp_p in self.peers:
            msg = ('READY', self.my_id)
            self.send_socket.sendto(pickle.dumps(msg), (ip, udp_p))

    def broadcast_messages(self):
        while True:
            with self.lock:
                if self.handshake_count >= N:
                    break
            time.sleep(0.1)

        chars = "abcdefghijklmnopqrstuvwxyz "
        for msg_num in range(self.num_msgs):
            time.sleep(random.uniform(0.01, 0.1))
            op = {
                "user": self.my_id,
                "type": random.choice(["INSERT", "DELETE"]),
                "char": random.choice(chars),
                "pos": random.randint(0, 100),
                "timestamp": time.time()
            }
            msg_pack = pickle.dumps(op)
            for ip, udp_p in self.peers:
                self.send_socket.sendto(msg_pack, (ip, udp_p))

        stop_msg = pickle.dumps({"type": "STOP"})
        for ip, udp_p in self.peers:
            self.send_socket.sendto(stop_msg, (ip, udp_p))

    def run(self):
        self.register_and_advertise()
        
        while True:
            self.wait_for_start_signal()
            if self.num_msgs == 0:
                # Remove do servidor de nomes antes de sair
                self.ns_stub.Unbind(name_service_pb2.UnbindRequest(name=self.my_name))
                break

            self.handshake_count = 0
            self.update_peer_list_via_directory()
            
            handler = MsgHandler(self)
            handler.start()

            self.broadcast_handshake()
            self.broadcast_messages()
            handler.join()

class MsgHandler(threading.Thread):
    def __init__(self, peer_node):
        super().__init__()
        self.node = peer_node
        self.log_list = []

    def run(self):
        # 1. Fase de Handshake
        while True:
            with self.node.lock:
                if self.node.handshake_count >= N:
                    break
            data = self.node.recv_socket.recv(1024)
            msg = pickle.loads(data)
            if isinstance(msg, tuple) and msg[0] == 'READY':
                with self.node.lock:
                    self.node.handshake_count += 1

        # 2. Fase de Recebimento de Edição
        stop_count = 0
        while stop_count < N:
            data = self.node.recv_socket.recv(1024)
            msg = pickle.loads(data)
            if isinstance(msg, dict) and msg.get("type") == "STOP":
                stop_count += 1
            else:
                self.log_list.append(msg)

        self.save_and_report_to_server()

    def save_and_report_to_server(self):
        # Encontra dinamicamente o Comparison Server via lookup
        try:
            lookup_res = self.node.ns_stub.Lookup(name_service_pb2.LookupRequest(name="ComparisonServer"))
            if lookup_res.success:
                srv_ip = lookup_res.address.ip
                srv_port = lookup_res.address.port
                with socket(AF_INET, SOCK_STREAM) as client_sock:
                    client_sock.connect((srv_ip, srv_port))
                    client_sock.send(pickle.dumps(self.log_list))
            else:
                print("Não foi possível localizar o ComparisonServer no Serviço de Nomes.")
        except Exception as e:
            print(f"Erro ao reportar logs: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: python peerCommunicatorUDP.py <NomeUnico> <PortaUDP> <PortaTCP>")
        sys.exit(1)
    
    name = sys.argv[1]
    u_port = int(sys.argv[2])
    t_port = int(sys.argv[3])
    
    peer = PeerNode(name, u_port, t_port)
    peer.run()
