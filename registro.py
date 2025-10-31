import grpc
from concurrent import futures
import sys
import os
from pymongo import MongoClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'proto'))
import reservas_pb2
import reservas_pb2_grpc

MONGODB_URI = "mongodb://localhost:27017"


class RegistroService(reservas_pb2_grpc.RegistroServiceServicer):
    
    def __init__(self, db_client):
        self.db_client = db_client
        self.db = db_client['restaurante']
        
    def RegistrarReserva(self, request, context):
        print(f"Registrando reserva: {request.reservation_id} para {request.name}")
        
        try:
            reservas_collection = self.db['reservas']
            
            reserva = {
                'reservation_id': request.reservation_id,
                'name': request.name,
                'phone': request.phone,
                'party_size': request.party_size,
                'preferences': request.preferences,
                'table_id': list(request.table_id),
                'status': request.status
            }
            
            reservas_collection.insert_one(reserva)
            
            mesas_collection = self.db['mesas']
            
            for table_id in request.table_id:
                mesas_collection.update_one(
                    {'table_id': table_id},
                    {'$set': {'status': 'Reservada'}}
                )
                print(f"Mesa {table_id} marcada como Reservada")
            
            print(f"✓ Reserva {request.reservation_id} registrada exitosamente")
            
            return reservas_pb2.RegistroResponse(
                success=True,
                mensaje="Reserva registrada correctamente"
            )
            
        except Exception as e:
            print(f"Error al registrar reserva: {e}")
            return reservas_pb2.RegistroResponse(
                success=False,
                mensaje=f"Error al registrar reserva: {str(e)}"
            )


def serve():
    print(f"Conectando a MongoDB en {MONGODB_URI}...")
    try:
        client = MongoClient(MONGODB_URI)
        client.admin.command('ping')
        print("✓ Conectado a MongoDB en MV2")
    except Exception as e:
        print(f"Error al conectar a MongoDB: {e}")
        return
    
    servicio = RegistroService(client)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    reservas_pb2_grpc.add_RegistroServiceServicer_to_server(servicio, server)
    server.add_insecure_port('[::]:50052')
    
    print("✓ Servicio de Registro escuchando en :50052")
    server.start()
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nDeteniendo servicio de registro...")
        server.stop(0)
        client.close()


if __name__ == '__main__':
    serve()

