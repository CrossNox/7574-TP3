from typing import Dict, Optional
import enum
import json


class MessageType(enum.Enum):
    # Start New Session
    SYN = 0 # Enviado por el cliente para solicitar nueva sesión
    SYNACK = 1 # Enviado por el servidor para aceptar solicitud del cliente (requiere confirmación)
    SYNCHECK = 2 # Enviado por el cliente para confirmar sesión
    CHECKACK = 3 # Enviado por el servidor para confirmar sesión al cliente
    # Ask for result
    RESULT = 4 # Enviado por el cliente para solicitar resultado al servidor
    RESRESP = 5 # Enviado por el servidor, con los resultados computados
    NOTDONE = 6 # Enviado por el servidor, para indicar resultados no computados
    # Finish current session
    FIN = 7 # Enviado por el cliente para solicitar fin de sesión al servidor
    FINACK = 8 # Enviado por el servidor, indicando que la sesión ha finalizado
    # Errors
    INVALMSG = 9 # Enviado por el servidor, para indicar que un mensaje es inválido
    NOTAVAIL = 10 # Enviado por el servidor, para indicar que no se encuentra disponible
    INVALSESSION = 11 # Enviado por el servidor, para indicar que el session_id enviado no es correcto
    # Redirection
    PROBE = 12 # Enviado por el cliente, para verificar que el servidor pueda recibir peticiones (que sea el lider)
    REDIRECT = 13 # Enviado por el servidor, para indicar el address del nodo disponible para recibir peticiones
    PROBEACK = 14 # Enviado por el servidor, para indicar que se encuentra disponible para recibir peticiones

NO_SESSION = -1 #TODO: Constantes a constants.py

class ProtocolMessage:
    def __init__(self, mtype: MessageType):
        self.mtype = mtype

class ClientMsg(ProtocolMessage):
    def __init__(
        self,
        mtype: MessageType,
        session_id: int = NO_SESSION
    ):
        super().__init__(mtype)
        self.session_id = session_id

    def encode(self) -> str:
        m = {
            'type': self.mtype.value,
            'session_id': self.session_id
        }
        return json.dumps(m)

    @classmethod
    def decode(cls, data: str):
        d_data = json.loads(data)
        m = ClientMsg(MessageType(d_data['type']), d_data['session_id'])
        return m

class ServerMsg(ProtocolMessage):
    def __init__(
        self,
        mtype: MessageType,
        payload: Optional[Dict] = None
    ):
        super().__init__(mtype)
        self.payload = payload or {}

    def encode(self) -> str:
        m = {
            'type': self.mtype.value,
            'payload': self.payload
        }
        return json.dumps(m)

    @classmethod
    def decode(cls, data: str):
        d_data = json.loads(data)
        m = ServerMsg(MessageType(d_data['type']), d_data['payload'])
        return m