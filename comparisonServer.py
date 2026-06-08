import socket
import pickle
import grpc
import name_service_pb2
import name_service_pb2_grpc
from constMP import NAME_SERVICE_ADDR, NAME_SERVICE_PORT, N

class ComparisonServer:
    def __init__(self, my_port=5678):
        self.port = my_port
        self.server_sock = None
        
        # Conexão gRPC
        self.channel = grpc.insecure_channel(f'{NAME_SERVICE_ADDR}:{NAME_SERVICE_PORT}')
        self.ns_stub = name_service_pb2_grpc.NameDirectoryServiceStub(self.channel)

    def __enter__(self):
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(('0.0.0.0', self.port))
        self.server_sock.listen(6)
        
        # Registra a si mesmo no Name Service para que os peers o localizem
        local_ip = self._get_local_ip()
        req = name_service_pb2.BindRequest(
            name="ComparisonServer",
            address=name_service_pb2.Address(ip=local_ip, port=self.port)
        )
        self.ns_stub.Bind(req)
        print(f"Comparison Server registrado sob o nome 'ComparisonServer' em {local_ip}:{self.port}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.server_sock:
            self.server_sock.close()
        self.ns_stub.Unbind(name_service_pb2.UnbindRequest(name="ComparisonServer"))

    def _get_local_ip(self):
        # Fallback simples para IP local utilizável na rede
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = '127.0.0.1'
        finally:
            s.close()
        return ip

    def _get_peers_via_directory(self):
        """Descobre dinamicamente os peers baseados no atributo/tipo"""
        req = name_service_pb2.DiscoverRequest(type="peer")
        res = self.ns_stub.Discover(req)
        return [(p.name, p.address.ip, p.address.port) for p in res.processes]

    def start_peers(self, peer_list, n_msgs):
        for idx, (name, ip, tcp_port) in enumerate(peer_list):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((ip, tcp_port))
                    s.send(pickle.dumps((idx, n_msgs)))
                    confirm = s.recv(512)
                    print(f"Confirmação de inicialização vinda de {name}: {pickle.loads(confirm)}")
            except Exception as e:
                print(f"Falha ao iniciar {name} em {ip}:{tcp_port} -> {e}")

    def wait_for_logs_and_compare(self, n_msgs):
        all_logs = []
        print(f"Aguardando logs de {N} peers...")

        while len(all_logs) < N:
            conn, addr = self.server_sock.accept()
            with conn:
                data = bytearray()
                while True:
                    packet = conn.recv(65536)
                    if not packet: break
                    data.extend(packet)
                log = pickle.loads(data)
                all_logs.append(log)
                print(f"Log recebido ({len(all_logs)}/{N})")

        self.compare_logs(all_logs, n_msgs)

    def compare_logs(self, all_logs, n_msgs):
        unordered_rounds = 0
        for j in range(n_msgs):
            try:
                reference_msg = all_logs[0][j]
                for p in range(1, N):
                    if all_logs[p][j]["user"] != reference_msg["user"] or all_logs[p][j]["pos"] != reference_msg["pos"]:
                        unordered_rounds += 1
                        break
            except IndexError:
                unordered_rounds += 1
                break

        print(f"\n--- Resultado da Comparação das Edições ---")
        print(f"Operações concorrentes fora de ordem detectadas: {unordered_rounds}")
        print(f"-------------------------------------------\n")

    def run(self):
        while True:
            try:
                n_msgs = int(input('Mensagens de edição por peer (0 para sair) => '))
            except ValueError:
                continue

            peer_list = self._get_peers_via_directory()
            print(f"Peers localizados via diretório gRPC: {[p[0] for p in peer_list]}")

            if n_msgs == 0:
                self.start_peers(peer_list, 0)
                break

            self.start_peers(peer_list, n_msgs)
            self.wait_for_logs_and_compare(n_msgs)

if __name__ == "__main__":
    with ComparisonServer() as server:
        server.run()
