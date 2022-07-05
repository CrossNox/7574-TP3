from typing import Dict, Optional, List
import enum
import zmq
import json
import time

#TODO: Cambiar prints por logger

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

import shelve

class Server:
    # address -> localhost:8000
    def __init__(self, port: str):
        self.context = zmq.Context.instance()  # type: ignore
        self.rep = self.context.socket(zmq.REP)
        self.rep.bind(f"tcp://*:{port}")
        self.current_session = NO_SESSION
        self.completed_sessions = 0                                                     #TODO: MANEJAR EXCEPCIONES Y FALLOS DE CONEXIÓN DEL LADO DEL SERVER
        self.on_creation_session = 0
        self.work_done_count = 0
        self.db = shelve.open('/app/db-data/server-db', writeback=True)
        self.load_state()

        print(f"Server started on {port}")

    # This is only for testing
    def write_state(self):
        state = {
            'completed_sessions': self.completed_sessions,
            'current_session': self.current_session,
            'work_done_count': self.work_done_count
        }

        self.db['state'] = state
        self.db.sync()

        print("State Written")

    def load_state(self):
        if 'state' not in self.db:
            return
        state = self.db['state']

        self.completed_sessions = state.get('completed_sessions', 0)
        self.current_session = state.get('current_session', 0)
        self.work_done_count = state.get('work_done_count', 0)

        print("Loading state from DB:")
        print(f"- Completed Sessions: {self.completed_sessions}")
        print(f"- Current Session: {self.current_session}")
        print(f"- Work Done Count: {self.work_done_count}")


    def run(self):
        while True:
            try:
                m = self.rep.recv_string()
                self.__handle_new_message(ClientMsg.decode(m))
            except Exception as e:
                print(f"Exception on server {e}")


    def __handle_new_message(self, msg: ClientMsg):
        handlers = {
            MessageType.PROBE: self.__handle_probe,
            MessageType.SYN: self.__handle_syn,
            MessageType.SYNCHECK: self.__handle_syncheck,
            MessageType.RESULT: self.__handle_result,
            MessageType.FIN: self.__handle_fin
        }

        if not msg.mtype in handlers:
            self.rep.send_string(ServerMsg(MessageType.INVALMSG).encode())
            return

        handler = handlers[msg.mtype]
        handler(msg)

    def __handle_probe(self, _msg: ClientMsg):
        print("Received PROBE")
        self.rep.send_string(ServerMsg(MessageType.PROBEACK).encode())

    def __handle_syn(self, _msg: ClientMsg):
        print("Received SYN")
        # We don't want to persist any data here, the session has not been created yet
        if self.current_session != NO_SESSION:
            self.rep.send_string(ServerMsg(MessageType.NOTAVAIL).encode())
            return

        self.on_creation_session = self.completed_sessions + 1

        session_data = {
            'session_id': self.on_creation_session,
        }

        self.rep.send_string(ServerMsg(MessageType.SYNACK, payload=session_data).encode())

    def __handle_syncheck(self, msg: ClientMsg):
        print("Received SYNCHECK")
        # Now, we received confirmation, so we will create the session
        if self.current_session != NO_SESSION:
            self.rep.send_string(ServerMsg(MessageType.INVALSESSION).encode())
            return

        if msg.session_id != self.on_creation_session:
            self.rep.send_string(ServerMsg(MessageType.INVALSESSION).encode())
            return

        # Now, let's send_string that data to disk!
        self.current_session = self.on_creation_session

        # TODO: This should not be hardcoded
        session_data = {
            'session_id': self.current_session,
            'data_address': 'rabbitmq',
            'posts_exchange': 'posts',
            'comments_exchange': 'comments',
            'posts_consumer_count': 3,
            'comments_consumer_count': 3,
        }

        # TODO: Persist here
        self.write_state()

        self.rep.send_string(ServerMsg(MessageType.CHECKACK, payload=session_data).encode())

    def __handle_result(self, msg: ClientMsg):
        print("Received RESULT")
        # Here we want to check if computation has finished
        if self.current_session != msg.session_id:
            self.rep.send_string(ServerMsg(MessageType.INVALSESSION).encode())
            return

        if not self.__work_done():
            print("Work not done")
            self.rep.send_string(ServerMsg(MessageType.NOTDONE).encode())
            return

        computation_result = self.__get_computation_results()

        self.rep.send_string(ServerMsg(MessageType.RESRESP, payload=computation_result).encode())



    # Fin will always response FINACK, except when it's on a leader-election
    def __handle_fin(self, msg: ClientMsg):
        print("Received FIN")
        if self.current_session != msg.session_id:
            self.rep.send_string(ServerMsg(MessageType.FINACK).encode())
            return

        # Reset current session, including computation results
        self.current_session = NO_SESSION
        self.completed_sessions += 1

        # TODO: Persist here
        self.write_state()

        self.rep.send_string(ServerMsg(MessageType.FINACK).encode()) # If this fails, the client will send another FIN


    def __work_done(self):
        self.work_done_count += 1
        return self.work_done_count % 10 == 0

    def __get_computation_results(self):
        return {
            'post_score_avg': 113.25,
            'best_meme': '-as9idj2jpoisadjc81jlkfsa',
            'education_memes': [
                'http://veryfunny.com',
                'http://veryfunny/school.com',
                'http://veryfunny/teaching.com',
            ]
        }

class Client:
    # address -> localhost:8000
    def __init__(self, addresses: List[str]):
        self.addresses = addresses
        self.context = zmq.Context.instance()  # type: ignore
        self.session_id = NO_SESSION
        self.req = None


    def run(self):
        self.__start_new_session()
        print("Writing data on queues...")
        time.sleep(10)
        # Here we should start sending comments and posts to queues
        # When we are done with that, we start asking server for the results
        self.__get_computation_result()
        self.__finish_session()

    def __close_connection(self):
        if self.req != None:
            self.req.close()
            self.req = None

    def __start_new_session(self):
        while True:
            print("Starting new session on server...")
            try:
                self.__connect_to_server()
                resp = self.__send_and_wait_response(ClientMsg(MessageType.SYN, NO_SESSION), retry=True)

                self.session_id = int(resp.payload['session_id'])
                resp = self.__send_and_wait_response(ClientMsg(MessageType.SYNCHECK, self.session_id), retry=True)

                if resp.mtype != MessageType.CHECKACK:
                    self.__log_response(resp)
                    self.__close_connection()
                    continue

                # TODO: Inicializar estructuras de rabbit
                print("New session has been created")
                return
            except Exception:
                pass

    def __get_computation_result(self):
        while True:
            print("Asking for computation results...")
            resp = self.__send_and_wait_response(ClientMsg(MessageType.RESULT, self.session_id), retry=True)

            if resp.mtype != MessageType.RESRESP:
                print("Computation hasn't finished yet")
                self.__close_connection()
                time.sleep(1)
                self.__connect_to_server()
                continue

            data = resp.payload

            print(f"Score Avg: {data['post_score_avg']}")
            print(f"Best Meme: {data['best_meme']}")
            print("Education Memes:")
            for meme in data['education_memes']:
                print(f" - {meme}")

            return

    def __finish_session(self):
        while True:
            print("Finishing session with server")

            resp = self.__send_and_wait_response(ClientMsg(MessageType.FIN, self.session_id), retry=True)

            if resp.mtype != MessageType.FINACK:
                print("Caution: Server do not recognize current session!")
                self.__close_connection()
                time.sleep(1)
                self.__connect_to_server()
                continue

            print("Session with server finished")

            return

    def __connect_to_server(self):
        tries = 0
        while True:
            for address in self.addresses:
                try:
                    self.__connect(address)
                    return
                except Exception as e:
                    print(f"Connection with server failed on address {address}")
                    print(e)
                    tries += 1
                    if tries == len(self.addresses):
                        tries = 0
                        #TODO: En realidad esto significa que el server es not-available, no debería pasar
                        # Si llega a quedar, deberiamos levantar el valor de config, no hardcodearlo
                        time.sleep(1)

    def __connect(self, address):
        while True:
            print(f"Trying to connect with server on address {address}")
            self.__close_connection()
            self.req = self.context.socket(zmq.REQ)
            self.req.connect(f"tcp://{address}")
            self.req.send_string(ClientMsg(MessageType.PROBE, NO_SESSION).encode())
            m = self.req.recv_string()
            resp = ServerMsg.decode(m)
            if resp.mtype == MessageType.REDIRECT:
                address = resp.payload['address']
                self.__close_connection()
                print(f"Being redirected to address {address}")
                continue
            elif resp.mtype == MessageType.NOTAVAIL:
                print("Server is not available, we wait...")
                time.sleep(1) # TODO: Change
                continue
            elif resp.mtype == MessageType.PROBEACK:
                print(f"Connected with server on address {address}")
                break
            else:
                raise Exception("Could not connect to server address")


    def __send_and_wait_response(self, msg: ClientMsg, retry=True) -> ServerMsg:
        while True:
            try:
                self.req.send_string(msg.encode())
                m = self.req.recv_string()
                resp = ServerMsg.decode(m)
                if not retry:
                    return resp

                if resp.mtype == MessageType.NOTAVAIL:
                    self.__log_response(resp)
                    self.__close_connection()
                    time.sleep(5) #TODO: Change
                    self.__connect_to_server()
                elif resp.mtype == MessageType.REDIRECT:
                    self.__log_response(resp)
                    self.__close_connection()
                    address = msg.payload['address']
                    self.__connect(address)
                else:
                    return resp

            except Exception: # TODO: En realidad acá deberíamos reconectar sólo si se rompió la conexión, para eso habría que leer la docu de zmq
                if not retry:
                    raise
                self.__connect_to_server()

    def __log_response(self, resp: ServerMsg):
        if resp.mtype == MessageType.INVALMSG:
            print("Invalid msg sent to server")
        elif resp.mtype == MessageType.NOTAVAIL:
            print("Server is not available right now")
        elif resp.mtype == MessageType.INVALSESSION:
            print("Session is incorrect")
