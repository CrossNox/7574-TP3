from typing import List
import zmq
import time
from lazarus.common.protocol import ServerMsg, ClientMsg, MessageType, NO_SESSION

class Client:
    # address -> localhost:8000
    def __init__(
        self, 
        hosts: List[str],
        port: str
    ):
        self.hosts = hosts
        self.port = port
        self.context = zmq.Context.instance()  # type: ignore
        self.context.setsockopt(zmq.LINGER, 0)
        self.session_id = NO_SESSION
        self.req = None


    def run(self):
        self.__start_new_session()
        print("Writing data on queues...")
        time.sleep(5)
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
                print(f"Respuesta de SYN, sesión asignada: {self.session_id}")
                resp = self.__send_and_wait_response(ClientMsg(MessageType.SYNCHECK, self.session_id), retry=True)
                print(f"Respuesta de SYNCHECK: {resp.mtype}")

                if resp.mtype != MessageType.CHECKACK:
                    self.__log_response(resp)
                    self.__close_connection()
                    continue

                # TODO: Inicializar estructuras de rabbit
                print("New session has been created")
                return
            except Exception as e:
                print(f"Excepción al intentar crear nueva sesión: {e}")

    def __get_computation_result(self):
        while True:
            print("Asking for computation results...")
            resp = self.__send_and_wait_response(ClientMsg(MessageType.RESULT, self.session_id), retry=True)

            if resp.mtype != MessageType.RESRESP:
                self.__handle_not_done()
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
            for host in self.hosts:
                try:
                    self.__connect(host)
                    return
                except Exception:
                    print("Connection with server failed")
                    tries += 1
                    if tries == len(self.hosts):
                        tries = 0
                        #TODO: En realidad esto significa que el server es not-available, no debería pasar
                        # Si llega a quedar, deberiamos levantar el valor de config, no hardcodearlo
                        time.sleep(1)

    def __connect(self, host):
        while True:
            address = f'{host}:{self.port}' 
            print(f"Trying to connect with server on address {address}")
            self.__close_connection()
            self.req = self.context.socket(zmq.REQ)
            self.req.RCVTIMEO = 1000 #TODO: Config
            self.req.SNDTIMEO = 1000 #TODO: Config
            self.req.connect(f"tcp://{address}")
            self.req.send_string(ClientMsg(MessageType.PROBE, NO_SESSION).encode())
            m = self.req.recv_string()
            resp = ServerMsg.decode(m)

            if resp.mtype == MessageType.REDIRECT:
                host = resp.payload['host']
                print(f"Being redirected to host {host}")
            elif resp.mtype == MessageType.NOTAVAIL:
                print("Server is not available right now")
                time.sleep(5) #TODO: Change
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
                    self.__handle_not_avail()
                elif resp.mtype == MessageType.REDIRECT:
                    self.__handle_redirection(msg)
                else:
                    return resp

            except Exception as e: # TODO: En realidad acá deberíamos reconectar sólo si se rompió la conexión, para eso habría que leer la docu de zmq
                if not retry:
                    raise
                self.__connect_to_server()
                print(f"Excepción al intentar enviar un mensaje {e}")

    def __handle_redirection(self, msg: ServerMsg):
        self.__close_connection()
        host = msg.payload['host']
        print(f"Being redirected to host {host}")
        self.__connect(host)


    def __handle_not_avail(self):
        print("Server is not available right now")
        self.__close_connection()
        time.sleep(5) #TODO: Change
        self.__connect_to_server()
    
    def __handle_not_done(self):
        print("Computation hasn't finished yet")
        self.__close_connection()
        time.sleep(5)
        self.__connect_to_server()


    def __log_response(self, resp: ServerMsg):
        if resp.mtype == MessageType.INVALMSG:
            print("Invalid msg sent to server")
        elif resp.mtype == MessageType.NOTAVAIL:
            print("Server is not available right now")
        elif resp.mtype == MessageType.INVALSESSION:
            print("Session is incorrect")