import random
from typing import List

import zmq

from lazarus.cfg import cfg
from lazarus.utils import get_logger
from lazarus.server.storage import ServerStorage
from lazarus.server.collector import ResultCollector
from lazarus.server.leader_election import LeaderElectionMock
from lazarus.common.protocol import LOG_TABLE, ClientMsg, ServerMsg, MessageType
from lazarus.constants import (
    NO_SESSION,
    DEFAULT_MOM_HOST,
    DEFAULT_SERVER_PORT,
    DEFAULT_POSTS_EXCHANGE,
    DEFAULT_COMMENTS_EXCHANGE,
)

SERVER_PORT: int = cfg.server_port(default=DEFAULT_SERVER_PORT, cast=int)
MOM_HOST: str = cfg.mom_host(default=DEFAULT_MOM_HOST)
POSTS_EXCHANGE: str = cfg.posts_exchange(default=DEFAULT_POSTS_EXCHANGE)
COMMENTS_EXCHANGE: str = cfg.posts_exchange(default=DEFAULT_COMMENTS_EXCHANGE)


MIN_SESSION_ID: int = 1
MAX_SESSION_ID: int = 100_000_000

logger = get_logger(__name__)


class Server:
    def __init__(
        self,
        s_id: int,
        group_identifier: str,
        group_size: int,
        posts_group: List[str],
        comments_group: List[str],
        results_queue: str,
    ):
        self.context = zmq.Context.instance()  # type: ignore
        self.context.setsockopt(zmq.LINGER, 0)
        self.rep = self.context.socket(zmq.REP)
        self.rep.bind(f"tcp://*:{SERVER_PORT}")
        self.current_session = NO_SESSION
        self.on_creation_session = 0
        self.posts_group = posts_group
        self.comments_group = comments_group

        # TODO: Leader election is hardcoded
        self.election = LeaderElectionMock()
        self.storage = ServerStorage(s_id, group_identifier, group_size)
        self.collector = ResultCollector(results_queue)
        self.result = None

        logger.info(f"Server started on {SERVER_PORT}")

    def run(self):
        self.election.wait_for_leader()
        i_was_leader = False

        while True:
            try:
                req = self.__receive()
                logger.info("a")
                if self.election.i_am_leader():
                    if not i_was_leader:
                        logger.info("b")
                        self.collector.start()
                        i_was_leader = True
                        logger.info("c")
                        self.__retrieve_state()
                        logger.info("d")
                    self.__handle_as_leader(req)
                else:
                    self.collector.stop()
                    i_was_leader = False
                    self.__handle_as_replica(req)

            except Exception as e:
                logger.error(f"Exception occurred on server: {e}", exc_info=True)

    def __retrieve_state(self):
        session, result = self.storage.retrieve_state()
        self.current_session = session
        self.result = result

    def __handle_as_replica(self, _msg: ClientMsg):
        # TODO: Posible bug, que el líder no sea el host
        leader = self.election.get_leader()  # pylint: disable=assignment-from-none

        if leader is None:
            self.__send(MessageType.NOTAVAIL)
        else:
            # TODO: Borrar este log
            logger.info(f"Redirecting to {leader}")
            leader = {"host": leader}
            self.__send(MessageType.REDIRECT, leader)

    def __handle_as_leader(self, msg: ClientMsg):
        logger.info("e")
        logger.info(msg)
        handlers = {
            MessageType.PROBE: self.__handle_probe,
            MessageType.SYN: self.__handle_syn,
            MessageType.SYNCHECK: self.__handle_syncheck,
            MessageType.RESULT: self.__handle_result,
            MessageType.FIN: self.__handle_fin,
        }

        if msg.mtype not in handlers:
            self.__send(MessageType.INVALMSG)
            return

        handler = handlers[msg.mtype]
        handler(msg)

    def __handle_probe(self, _msg: ClientMsg):
        self.__send(MessageType.PROBEACK)

    def __get_session_identifier(self):
        return random.randint(MIN_SESSION_ID, MAX_SESSION_ID)

    def __handle_syn(self, _msg: ClientMsg):
        # We don't want to persist any data here, the session has not been created yet
        if self.current_session != NO_SESSION:
            self.__send(MessageType.NOTAVAIL)
            return

        self.on_creation_session = self.__get_session_identifier()

        session_data = {
            "session_id": self.on_creation_session,
        }

        self.__send(MessageType.SYNACK, session_data)

    def __handle_syncheck(self, msg: ClientMsg):
        # If the msg session_id is equal to the one on creation, then we confirm the session
        if self.current_session != msg.session_id:
            if (
                self.current_session != NO_SESSION
                or msg.session_id != self.on_creation_session
            ):
                self.__send(MessageType.INVALSESSION)
                return

            self.current_session = self.on_creation_session

        session_data = {
            "session_id": self.current_session,
            "address": MOM_HOST,
            "posts_exchange": POSTS_EXCHANGE,
            "comments_exchange": COMMENTS_EXCHANGE,
            "posts_groups": self.posts_group,
            "comments_groups": self.comments_group,
        }

        self.storage.new_session(self.current_session)

        self.__send(MessageType.CHECKACK, session_data)

    def __handle_result(self, msg: ClientMsg):
        # Here we want to check if computation has finished
        if self.current_session != msg.session_id:
            self.__send(MessageType.INVALSESSION)
            return

        if self.result is not None:
            self.__send(MessageType.RESRESP, self.result)
            return

        result = self.collector.try_get_result(self.current_session)

        if result is None:
            self.__send(MessageType.NOTDONE)
            return

        self.storage.add_result(result)
        self.collector.ack()
        self.__send(MessageType.RESRESP, result)

    # Fin will always response FINACK, except when it's on a leader-election
    def __handle_fin(self, msg: ClientMsg):
        if self.current_session != msg.session_id:
            self.__send(MessageType.FINACK)
            return

        # Reset current session, including computation results
        self.current_session = NO_SESSION
        self.result = None

        self.storage.finish_session(self.current_session)

        self.__send(MessageType.FINACK)

    def __send(self, mtype: MessageType, payload=None):
        if mtype not in LOG_TABLE:
            logger.error(f"Error, trying to send invalid msg {mtype}")
            return
        logger.info(f"Sending {LOG_TABLE[mtype]}")
        self.rep.send_string(ServerMsg(mtype, payload=payload).encode())

    def __receive(self) -> ClientMsg:
        try:
            m = self.rep.recv_string()
            msg = ClientMsg.decode(m)
            self.__log_msg(msg)
            return msg
        except Exception as e:
            logger.error(f"Error receiving a new message: {e}")

    def __log_msg(self, msg: ClientMsg):
        msg_type = "unknown"
        session_id = "unknown session id"
        if msg.mtype in LOG_TABLE:
            msg_type = LOG_TABLE[msg.mtype]

        if msg.session_id != NO_SESSION:
            session_id = f"session id {msg.session_id}"

        logger.info(f"Received {msg_type} with {session_id}")
