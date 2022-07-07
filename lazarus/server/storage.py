from threading import Event
from typing import Dict, List, Union

from lazarus.cfg import cfg
from lazarus.mom.queue import Queue
from lazarus.utils import get_logger
from lazarus.mom.message import Message
from lazarus.storage.local import LocalStorage
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.constants import (
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

        # TODO: Replace this with build_node_identifier on integration
        self.identifier = f"{group_identifier}{s_id}"

        self.exchange = WorkerExchange(
            MOM_HOST,
            DB_EXCHANGE,
            [
                # TODO: Replace this with build_node_identifier on integration
                ConsumerConfig(f"{group_identifier}{i}", ConsumerType.Subscriber)
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
        # TODO: Replace this with build_node_identifier and DEFAULT_PATH on integration
        storage = LocalStorage.load(f"/data/{self.identifier}")

        # TokenMessage
        token = {"type": "token", "data": self.identifier}

        self.exchange.push(Message(token))

        # Now we consume the queue until we find the token
        finished = Event()

        def __callback(msg: Message):
            mtype = msg["type"]
            payload = msg["payload"]

            if mtype == "token":
                if payload == self.identifier:
                    msg.ack()
                    finished.set()
                    return
                # else ignored
            elif mtype == "new_session":
                storage.put("session_id", payload, topic=DB_TOPIC)
            elif mtype == "finish_session":
                storage.put("session_id", NO_SESSION, topic=DB_TOPIC)
            elif mtype == "result":
                storage.put("result", payload, topic=DB_TOPIC)
            else:
                logger.error(
                    f"Received unknown message of type {mtype} on ServerStorage"
                )

            msg.ack()

        queue = Queue(MOM_HOST, self.identifier)
        queue.consume(__callback)
        finished.wait()
        queue.close()

        # Now we recover state from db
        session_id = storage.get("session_id", topic=DB_TOPIC)
        result = storage.get("result", topic=DB_TOPIC)

        return session_id, result

    def close(self):
        self.exchange.close()
