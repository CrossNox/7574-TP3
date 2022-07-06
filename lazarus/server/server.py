import shelve

import zmq

from lazarus.cfg import cfg
from lazarus.utils import get_logger
from lazarus.constants import NO_SESSION, DEFAULT_SERVER_PORT
from lazarus.common.protocol import LOG_TABLE, ClientMsg, ServerMsg, MessageType

SERVER_PORT: int = cfg.server_port(default=DEFAULT_SERVER_PORT, cast=int)


logger = get_logger(__name__)


class Server:
    def __init__(self, is_leader: bool):
        self.context = zmq.Context.instance()  # type: ignore
        self.context.setsockopt(zmq.LINGER, 0)
        self.rep = self.context.socket(zmq.REP)
        self.rep.bind(f"tcp://*:{SERVER_PORT}")
        self.current_session = NO_SESSION
        self.completed_sessions = 0
        self.on_creation_session = 0
        self.work_done_count = 0
        self.is_leader = is_leader
        if is_leader:
            self.db = shelve.open("/app/db-data/server-db", writeback=True)
            self.load_state()

        logger.info(f"Server started on {SERVER_PORT}")

    # TODO: Remove both write_state and load_state
    def write_state(self):
        state = {
            "completed_sessions": self.completed_sessions,
            "current_session": self.current_session,
            "work_done_count": self.work_done_count,
        }

        self.db["state"] = state
        self.db.sync()

        print("State Written")

    def load_state(self):
        if "state" not in self.db:
            return
        state = self.db["state"]

        self.completed_sessions = state.get("completed_sessions", 0)
        self.current_session = state.get("current_session", 0)
        self.work_done_count = state.get("work_done_count", 0)

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
                logger.info(f"Exception occurred on server: {e}")

    def __handle_new_message(self, msg: ClientMsg):
        self.log_msg(msg)

        # TODO: Remove hardcode
        if not self.is_leader:
            leader = {"host": "server1"}
            self.send(MessageType.REDIRECT, leader)
            return

        handlers = {
            MessageType.PROBE: self.__handle_probe,
            MessageType.SYN: self.__handle_syn,
            MessageType.SYNCHECK: self.__handle_syncheck,
            MessageType.RESULT: self.__handle_result,
            MessageType.FIN: self.__handle_fin,
        }

        if msg.mtype not in handlers:
            self.send(MessageType.INVALMSG)
            return

        handler = handlers[msg.mtype]
        handler(msg)

    def __handle_probe(self, _msg: ClientMsg):
        self.send(MessageType.PROBEACK)

    def __handle_syn(self, _msg: ClientMsg):
        # We don't want to persist any data here, the session has not been created yet
        if self.current_session != NO_SESSION:
            self.send(MessageType.NOTAVAIL)
            return

        self.on_creation_session = self.completed_sessions + 1

        session_data = {
            "session_id": self.on_creation_session,
        }

        self.send(MessageType.SYNACK, session_data)

    def __handle_syncheck(self, msg: ClientMsg):
        # Queremos que si el id del mensaje coincide con el de la sesión actual, confirmarle
        if self.current_session != msg.session_id:
            if (
                self.current_session != NO_SESSION
                or msg.session_id != self.on_creation_session
            ):
                self.send(MessageType.INVALSESSION)
                return

            self.current_session = self.on_creation_session

        # TODO: This should not be hardcoded
        session_data = {
            "session_id": self.current_session,
            "data_address": "rabbitmq",
            "posts_exchange": "posts",
            "comments_exchange": "comments",
            "posts_consumer_count": 3,
            "comments_consumer_count": 3,
        }

        # TODO: Persist here
        self.write_state()

        self.send(MessageType.CHECKACK, session_data)

    def __handle_result(self, msg: ClientMsg):
        # Here we want to check if computation has finished
        if self.current_session != msg.session_id:
            self.send(MessageType.INVALSESSION)
            return

        if not self.__work_done():
            self.send(MessageType.NOTDONE)
            return

        computation_result = self.__get_computation_results()

        self.send(MessageType.RESRESP, computation_result)

    # Fin will always response FINACK, except when it's on a leader-election
    def __handle_fin(self, msg: ClientMsg):
        if self.current_session != msg.session_id:
            self.send(MessageType.FINACK)
            return

        # Reset current session, including computation results
        self.current_session = NO_SESSION
        self.completed_sessions += 1

        # TODO: Persist here
        self.write_state()

        self.send(MessageType.FINACK)

    def send(self, mtype: MessageType, payload=None):
        if mtype not in LOG_TABLE:
            logger.error(f"Error, trying to send invalid msg {mtype}")
            return
        logger.info(f"Sending {LOG_TABLE[mtype]}")
        self.rep.send_string(ServerMsg(mtype, payload=payload).encode())

    def log_msg(self, msg: ClientMsg):
        msg_type = "unknown"
        session_id = "unknown session id"
        if msg.mtype in LOG_TABLE:
            msg_type = LOG_TABLE[msg.mtype]

        if msg.session_id != NO_SESSION:
            session_id = f"session id {msg.session_id}"

        logger.info(f"Received {msg_type} with {session_id}")

    def __work_done(self):
        self.work_done_count += 1
        return self.work_done_count % 10 == 0

    def __get_computation_results(self):
        return {
            "post_score_avg": 113.25,
            "best_meme": "-as9idj2jpoisadjc81jlkfsa",
            "education_memes": [
                "http://veryfunny.com",
                "http://veryfunny/school.com",
                "http://veryfunny/teaching.com",
            ],
        }
