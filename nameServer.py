import grpc
from concurrent import futures
import threading
import name_service_pb2
import name_service_pb2_grpc
from constMP import NAME_SERVICE_PORT

class NameDirectoryServicer(name_service_pb2_grpc.NameDirectoryServiceServicer):
  def __init__(self):
      self.lock = threading.Lock()
      self.names_db = {}  # nome -> {"ip": str, "port": int}
      self.types_db = {}  # nome -> tipo

  def Bind(self, request, context):
      with self.lock:
          if request.name in self.names_db:
              return name_service_pb2.StatusResponse(success=False, error_message="Nome já registrado.")
          
          self.names_db[request.name] = {
              "ip": request.address.ip,
              "port": request.address.port
          }
          print(f"[BIND] {request.name} associado a {request.address.ip}:{request.address.port}")
          return name_service_pb2.StatusResponse(success=True)

  def Lookup(self, request, context):
      with self.lock:
          if request.name not in self.names_db:
              return name_service_pb2.AddressResponse(success=False, error_message="Nome não encontrado.")
          
          addr = self.names_db[request.name]
          return name_service_pb2.AddressResponse(
              success=True,
              address=name_service_pb2.Address(ip=addr["ip"], port=addr["port"])
          )

  def Unbind(self, request, context):
      with self.lock:
          if request.name in self.names_db:
              del self.names_db[request.name]
              if request.name in self.types_db:
                  del self.types_db[request.name]
              print(f"[UNBIND] {request.name} removido do sistema.")
              return name_service_pb2.StatusResponse(success=True)
          return name_service_pb2.StatusResponse(success=False, error_message="Nome não existe.")

  def RegisterType(self, request, context):
      with self.lock:
          if request.name not in self.names_db:
              return name_service_pb2.StatusResponse(success=False, error_message="Nome precisa estar em Bind primeiro.")
          
          self.types_db[request.name] = request.type
          print(f"[REGISTER] {request.name} categorizado como tipo '{request.type}'")
          return name_service_pb2.StatusResponse(success=True)

  def Discover(self, request, context):
      with self.lock:
          results = []
          for name, p_type in self.types_db.items():
              if p_type == request.type and name in self.names_db:
                  addr = self.names_db[name]
                  proc_info = name_service_pb2.ProcessInfo(
                      name=name,
                      address=name_service_pb2.Address(ip=addr["ip"], port=addr["port"])
                  )
                  results.append(proc_info)
          return name_service_pb2.DiscoverResponse(processes=results)

def serve():
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  name_service_pb2_grpc.add_NameDirectoryServiceServicer_to_server(NameDirectoryServicer(), server)
  server.add_insecure_port(f'0.0.0.0:{NAME_SERVICE_PORT}')
  print(f"gRPC Name & Directory Service rodando na porta {NAME_SERVICE_PORT}...")
  server.start()
  server.wait_for_termination()

if __name__ == "__main__":
  serve()
