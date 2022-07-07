from threading import Event
from typing import Dict, List, Union

from lazarus.cfg import cfg
from lazarus.mom.queue import Queue
from lazarus.utils import build_node_id, ensure_path, get_logger
from lazarus.mom.message import Message
from lazarus.storage.local import LocalStorage
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.constants import (
    DEFAULT_DATA_DIR,
    NO_SESSION,
    DEFAULT_MOM_HOST,
    DEFAULT_SERVER_DB_TOPIC,
    DEFAULT_SERVER_DB_EXCHANGE,
)

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
                ConsumerConfig(build_node_id(group_identifier, i), ConsumerType.Subscriber)
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
        storage = LocalStorage.load(cfg.lazarus.data_dir(cast=ensure_path, default=DEFAULT_DATA_DIR) / self.identifier)

        # TokenMessage
        token = {"type": "token", "data": self.identifier}

        logger.info("before push")
        self.exchange.push(Message(token))
        logger.info("after push")

        # Now we consume the queue until we find the token
        finished = Event()

        def __callback(msg: Message):
            logger.info("im on the call back oh yes")
            try:
                mtype = msg["type"]
                data = msg["data"]
                logger.error(data)
                logger.error(self.identifier)
                if mtype == "token":
                    if data == self.identifier:
                        msg.ack()
                        finished.set()
                        return
                    # else ignored
                elif mtype == "new_session":
                    storage.put("session_id", data, topic=DB_TOPIC)
                elif mtype == "finish_session":
                    storage.put("session_id", NO_SESSION, topic=DB_TOPIC)
                elif mtype == "result":
                    storage.put("result", data, topic=DB_TOPIC)
                else:
                    logger.error(
                        f"Received unknown message of type {mtype} on ServerStorage"
                    )
                msg.ack()
            except:
                logger.error('never set', exc_info=True)

        logger.info("before queue")
        queue = Queue(MOM_HOST, self.identifier)
        logger.info("before consume")
        queue.consume(__callback)
        logger.info("after consume")
        finished.wait()
        logger.info("after wait")
        queue.close()

        # Now we recover state from db
        session_id = storage.get("session_id", topic=DB_TOPIC)
        result = storage.get("result", topic=DB_TOPIC)

        return session_id, result

    def close(self):
        self.exchange.close()
