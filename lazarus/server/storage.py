from multiprocessing import Event
from typing import Dict, List, Union

from lazarus.cfg import cfg
from lazarus.constants import (
    NO_SESSION,
    DEFAULT_DATA_DIR,
    DEFAULT_MOM_HOST,
    DEFAULT_SERVER_DB_TOPIC,
    DEFAULT_SERVER_DB_EXCHANGE,
)
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.mom.message import Message
from lazarus.mom.queue import Queue
from lazarus.storage.local import LocalStorage
from lazarus.utils import coalesce, get_logger, ensure_path, build_node_id

MOM_HOST: str = cfg.mom_host(default=DEFAULT_MOM_HOST)
DB_EXCHANGE: str = cfg.server_db_exchange(default=DEFAULT_SERVER_DB_EXCHANGE)
DB_TOPIC: str = cfg.server_db_topic(default=DEFAULT_SERVER_DB_TOPIC)


logger = get_logger(__name__)


ResultType = Dict[str, Union[List[str], str]]


class ServerStorage:
    def __init__(self, s_id: int, group_identifier: str, group_size: int):
        self.identifier = build_node_id(group_identifier, s_id)

        self.exchange = WorkerExchange(
            MOM_HOST,
            DB_EXCHANGE,
            [
                ConsumerConfig(
                    build_node_id(group_identifier, i), ConsumerType.Subscriber
                )
                for i in range(group_size)
            ],
        )

    def new_session(self, session_id: int):
        payload = {"type": "new_session", "data": session_id}

        self.exchange.push(Message(payload))

    def finish_session(self, session_id: int):
        payload = {"type": "finish_session", "data": session_id}

        self.exchange.push(Message(payload))

    def add_result(self, result: ResultType):
        payload = {"type": "result", "data": result}

        self.exchange.push(Message(payload))

    def retrieve_state(self):
        storage = LocalStorage.load(
            cfg.lazarus.data_dir(cast=ensure_path, default=DEFAULT_DATA_DIR)
            / self.identifier
        )

        # TokenMessage
        token = {"type": "token", "data": self.identifier}

        self.exchange.push(Message(token))

        # Now we consume the queue until we find the token

        class DummyCallback:
            def __init__(
                self, identifier, finished, storage,
            ):
                self.identifier = identifier
                self.finished = finished
                self.storage = storage

            def __call__(self, msg: Message):
                try:
                    mtype = msg["type"]
                    data = msg["data"]
                    logger.info("Got message %s", msg)
                    if mtype == "token":
                        if data == self.identifier:
                            msg.ack()
                            self.finished.set()
                            return
                        # else ignored
                    elif mtype == "new_session":
                        self.storage.put("session_id", data, topic=DB_TOPIC)
                    elif mtype == "finish_session":
                        self.storage.put("session_id", NO_SESSION, topic=DB_TOPIC)
                    elif mtype == "result":
                        self.storage.put("result", data, topic=DB_TOPIC)
                    else:
                        logger.error(
                            f"Received unknown message of type {mtype} on ServerStorage"
                        )
                    msg.ack()
                except Exception:
                    logger.error("never set", exc_info=True)

        finished = Event()

        __callback = DummyCallback(self.identifier, finished, storage)

        queue = Queue(MOM_HOST, self.identifier)
        queue.consume(__callback)
        finished.wait()
        logger.info("Finished! Closing queue")
        queue.close()
        logger.info("Queue closed")

        # Now we recover state from db
        session_id = coalesce(storage.get, log=False)("session_id", topic=DB_TOPIC)
        result = coalesce(storage.get, log=False)("result", topic=DB_TOPIC)

        if session_id is None:
            session_id = NO_SESSION

        return session_id, result

    def close(self):
        self.exchange.close()
