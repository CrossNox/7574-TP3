import time
import random
from typing import List
from pathlib import Path

import zmq

from lazarus.cfg import cfg
from lazarus.client.file_provider import FileProvider
from lazarus.common.protocol import ClientMsg, ServerMsg, MessageType
from lazarus.utils import get_logger, ascii_to_binary, ensure_file_directory
from lazarus.constants import (
    NO_SESSION,
    DEFAULT_MEME_PATH,
    DEFAULT_SERVER_PORT,
    DEFAULT_PROTOCOL_TIMEOUT,
    DEFAULT_PROTOCOL_RETRY_SLEEP,
)

RETRY_SLEEP: int = cfg.protocol_retry_sleep(
    default=DEFAULT_PROTOCOL_RETRY_SLEEP, cast=int
)

SERVER_PORT: int = cfg.server_port(default=DEFAULT_SERVER_PORT, cast=int)

TIMEOUT: int = cfg.server_port(default=DEFAULT_PROTOCOL_TIMEOUT, cast=int) * 10000

MEME_PATH: Path = cfg.meme_path(default=DEFAULT_MEME_PATH, cast=ensure_file_directory)

logger = get_logger(__name__)


class Client:
    def __init__(
        self,
        hosts: List[str],
        posts_path: Path,
        comments_path: Path,
    ):
        self.hosts = hosts
        self.posts_path = posts_path
        self.comments_path = comments_path
        self.context = zmq.Context.instance()  # type: ignore
        self.context.setsockopt(zmq.LINGER, 0)
        self.session_id = NO_SESSION
        self.req = None

    def run(self):
        session = self.__start_new_session()

        address = session.payload["address"]
        posts_exchange = session.payload["posts_exchange"]
        comments_exchange = session.payload["comments_exchange"]
        posts_groups = session.payload["posts_groups"]
        comments_groups = session.payload["comments_groups"]

        logger.info("Starting processes")

        pposts = FileProvider(
            self.session_id,
            address,
            posts_exchange,
            self.posts_path,
            posts_groups or [],
        )

        pcomments = FileProvider(
            self.session_id,
            address,
            comments_exchange,
            self.comments_path,
            comments_groups or [],
        )

        logger.info("Starting posts relay process")
        pposts.start()

        logger.info("Starting comments relay process")
        pcomments.start()

        pposts.join()
        logger.info("Joined posts relay process")

        pcomments.join()
        logger.info("Joined comments relay process")

        self.__get_computation_result()
        self.__finish_session()
        self.__close()

    def __close(self):
        self.__close_connection()
        self.context.term()  # TODO: Check if this is ok

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

                logger.info(f"New session has been created with id {self.session_id}")

                return resp
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

            logger.info(f"Score Avg: {data['posts_score_avg']}")
            logger.info("Education Memes:")
            for meme in data["education_memes"]:
                logger.info(f" - {meme}")

            best_meme = ascii_to_binary(data["best_meme"])
            logger.info(f"Downloading best meme to {MEME_PATH}")

            with open(MEME_PATH, "wb") as meme_file:
                meme_file.write(best_meme)
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
        except Exception as e:
            logger.info(e)
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
