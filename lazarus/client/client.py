import time
import random
from typing import List

import zmq

from lazarus.cfg import cfg
from lazarus.utils import get_logger
from lazarus.common.protocol import ClientMsg, ServerMsg, MessageType
from lazarus.constants import (
    NO_SESSION,
    DEFAULT_SERVER_PORT,
    DEFAULT_PROTOCOL_TIMEOUT,
    DEFAULT_PROTOCOL_RETRY_SLEEP,
)

RETRY_SLEEP: int = cfg.protocol_retry_sleep(
    default=DEFAULT_PROTOCOL_RETRY_SLEEP, cast=int
)

SERVER_PORT: int = cfg.server_port(default=DEFAULT_SERVER_PORT, cast=int)

TIMEOUT: int = cfg.server_port(default=DEFAULT_PROTOCOL_TIMEOUT, cast=int) * 1000


logger = get_logger(__name__)


class Client:
    def __init__(self, hosts: List[str]):
        self.hosts = hosts
        self.context = zmq.Context.instance()  # type: ignore
        self.context.setsockopt(zmq.LINGER, 0)
        self.session_id = NO_SESSION
        self.req = None

    def run(self):
        self.__start_new_session()
        logger.info("Writing data on queues...")
        # Here we should start sending comments and posts to queues
        # When we are done with that, we start asking server for the results
        time.sleep(5)  # TODO: Delete this, it's only to simulate work
        self.__get_computation_result()
        self.__finish_session()

    def __close_connection(self):
        if self.req is not None:
            self.req.close()
            self.req = None

    def __start_new_session(self):
        logger.info("Starting new session on server...")
        while True:
            try:
                self.__connect_to_server()
                resp = self.__send_and_wait_response(
                    ClientMsg(MessageType.SYN, NO_SESSION), retry=True
                )

                self.session_id = int(resp.payload["session_id"])
                resp = self.__send_and_wait_response(
                    ClientMsg(MessageType.SYNCHECK, self.session_id), retry=True
                )

                if resp.mtype != MessageType.CHECKACK:
                    self.__close_connection()
                    continue

                # TODO: Inicializar estructuras de rabbit
                logger.info(f"New session has been created with id {self.session_id}")
                return
            except Exception as e:
                logger.error(f"Exception trying to connet to server: {e}")

    def __get_computation_result(self):
        while True:
            logger.info("Asking server for computation results...")
            resp = self.__send_and_wait_response(
                ClientMsg(MessageType.RESULT, self.session_id), retry=True
            )

            if resp.mtype != MessageType.RESRESP:
                self.__handle_not_done()
                continue

            data = resp.payload

            # TODO: Delete this
            print(f"Score Avg: {data['post_score_avg']}")
            print(f"Best Meme: {data['best_meme']}")
            print("Education Memes:")
            for meme in data["education_memes"]:
                print(f" - {meme}")

            return

    def __finish_session(self):
        while True:
            logger.info("Finishing session with server")

            resp = self.__send_and_wait_response(
                ClientMsg(MessageType.FIN, self.session_id), retry=True
            )

            if resp.mtype != MessageType.FINACK:
                logger.error("Caution: Server do not recognize current session!")
                self.__close_connection()
                time.sleep(RETRY_SLEEP)
                self.__connect_to_server()
                continue

            logger.info("Session with server finished")

            return

    # def __connect_to_server_old(self):
    #     tries = 0
    #     while True:
    #         for host in self.hosts:
    #             try:
    #                 self.__connect(host)
    #                 return
    #             except Exception:
    #                 tries += 1
    #                 if tries < len(self.hosts):
    #                     logger.info("Connection with server failed, retrying...")
    #                 else:
    #                     tries = 0
    #                     logger.error("All known server hosts reported down, waiting...")
    #                     time.sleep(RETRY_SLEEP)

    # def __connect_old(self, host):
    #     while True:
    #         address = f'{host}:{self.port}'
    #         logger.info(f"Trying to connect with server on address {address}")
    #         self.__close_connection()
    #         self.req = self.context.socket(zmq.REQ)
    #         self.req.RCVTIMEO = TIMEOUT
    #         self.req.SNDTIMEO = TIMEOUT
    #         self.req.connect(f"tcp://{address}")
    #         self.req.send_string(ClientMsg(MessageType.PROBE, NO_SESSION).encode())
    #         m = self.req.recv_string()
    #         resp = ServerMsg.decode(m)

    #         if resp.mtype == MessageType.REDIRECT:
    #             host = resp.payload['host']
    #             logger.info(f"Being redirected to host {host}")
    #         elif resp.mtype == MessageType.NOTAVAIL:
    #             logger.info("Server is not available right now, waiting...")
    #             time.sleep(RETRY_SLEEP)
    #         elif resp.mtype == MessageType.PROBEACK:
    #             logger.info(f"Connected with server on address {address}")
    #             break
    #         else:
    #             raise Exception("Could not connect to server address")

    def get_hosts(self):
        hosts = list(self.hosts)
        random.shuffle(hosts)
        return hosts

    def __connect_to_server(self, on_host=None):
        not_visited = self.get_hosts()
        host = self.hosts[0]
        if on_host:
            not_visited.remove(on_host)
            host = on_host
        else:
            host = not_visited.pop()

        while True:
            resp = self.__try_connection(host)
            if resp is None:
                if len(not_visited) == 0:
                    logger.error("All known server hosts reported down, waiting...")
                    time.sleep(RETRY_SLEEP)
                    not_visited = self.get_hosts()
                host = not_visited.pop()
                continue

            not_visited = self.get_hosts()
            if resp.mtype == MessageType.REDIRECT:
                host = resp.payload["host"]
                logger.info(f"Being redirected to host {host}")
                not_visited.remove(host)
            elif resp.mtype == MessageType.NOTAVAIL:
                logger.info("Server is not available right now, waiting...")
                time.sleep(RETRY_SLEEP)
            elif resp.mtype == MessageType.PROBEACK:
                logger.info(f"Connected with server on host {host}")
                return

    def __try_connection(self, host):
        try:
            address = f"{host}:{SERVER_PORT}"
            logger.info(f"Trying to connect with server host {host}")
            self.__close_connection()
            self.req = self.context.socket(zmq.REQ)
            self.req.RCVTIMEO = TIMEOUT
            self.req.SNDTIMEO = TIMEOUT
            self.req.connect(f"tcp://{address}")
            self.req.send_string(ClientMsg(MessageType.PROBE, NO_SESSION).encode())
            m = self.req.recv_string()
            resp = ServerMsg.decode(m)

            return resp
        except Exception:
            logger.info("Connection with server failed")
            self.__close_connection()
            return None

    def __send_and_wait_response(self, msg: ClientMsg, retry=True) -> ServerMsg:
        while True:
            try:
                self.req.send_string(msg.encode())  # type: ignore
                m = self.req.recv_string()  # type: ignore
                resp = ServerMsg.decode(m)
                if not retry:
                    return resp

                if resp.mtype == MessageType.NOTAVAIL:
                    self.__handle_not_avail()
                elif resp.mtype == MessageType.REDIRECT:
                    self.__handle_redirection(msg)
                else:
                    return resp

            except Exception:
                if not retry:
                    raise
                logger.info("Lost connection with server, retrying...")
                self.__connect_to_server()

    def __handle_redirection(self, msg: ServerMsg):
        self.__close_connection()
        host = msg.payload["host"]
        logger.info(f"Being redirected to host {host}")
        self.__connect_to_server(host)

    def __handle_not_avail(self):
        logger.info("Server is not available right now, we wait...")
        self.__close_connection()
        time.sleep(RETRY_SLEEP)
        self.__connect_to_server()

    def __handle_not_done(self):
        logger.info("Computation hasn't finished yet, we wait...")
        self.__close_connection()
        time.sleep(RETRY_SLEEP)
        self.__connect_to_server()
