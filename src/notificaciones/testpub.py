import pulsar, _pulsar
from pulsar.schema import *
import uuid
import time
import os
import datetime

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0

def time_millis():
    return int(time.time() * 1000)


class EventoIntegracion(Record):
    id = String(default=str(uuid.uuid4()))
    time = Long()
    ingestion = Long(default=time_millis())
    specversion = String()
    type = String()
    datacontenttype = String()
    service_name = String()

class ReservaCreadaPayload(Record):
    id_reserva = String()
    id_cliente = String()
    estado = String()
    fecha_creacion = Long()

class EventoReservaCreada(EventoIntegracion):
    data = ReservaCreadaPayload()

HOSTNAME = os.getenv('PULSAR_ADDRESS', default="localhost")

client = pulsar.Client(f'pulsar://{HOSTNAME}:6650')
publicador = client.create_producer('eventos-reserva', schema=AvroSchema(EventoReservaCreada))
payload = ReservaCreadaPayload(
            id_reserva=str(123456), 
            id_cliente=str(1122334455), 
            estado=str('string state'), 
            fecha_creacion=int(250)
        )
evento_integracion = EventoReservaCreada(data=payload)
publicador.send(evento_integracion)


client.close()