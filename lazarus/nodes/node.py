from multiprocessing import Process
from typing import Dict, Type, TypeVar, Optional, Sequence

from lazarus.constants import EOS
from lazarus.exceptions import IncorrectSessionId
from lazarus.mom.exchange import Exchange
from lazarus.mom.message import Message
from lazarus.mom.queue import Queue
from lazarus.tasks.base import Task
from lazarus.utils import get_logger

logger = get_logger(__name__)

DependencyName = str
Host = str

TaskCls = TypeVar("TaskCls", bound=Task)


class Node(Process):
    def __init__(
        self,
        callback: Type[TaskCls],
        queue_in: Queue,
        exchanges_out: Sequence[Exchange],
        producers: int = 1,
        dependencies: Optional[Dict[DependencyName, Host]] = None,
        **callback_kwargs,
    ):
        super().__init__()
        self.current_session_id: Optional[str] = None
        self.dependencies = self.wait_for_dependencies(dependencies or {})
        self.callback = callback(**self.dependencies, **callback_kwargs)
        self.producers = producers
        self.n_eos = 0
        self.queue_in = queue_in
        self.exchanges_out = exchanges_out
        self.processed = 0

    def wait_for_dependencies(self, dependencies):
        solved_dependencies = {}
        for dependency, host in dependencies.items():
            solved_dependencies[dependency] = self.fetch_result(host)
        return solved_dependencies

    def fetch_result(self, host):
        # TODO: actually implement this
        return host

    def put_new_message_out(self, message: Dict) -> None:
        # TODO: decorate message with session id and type (data?)
        for exchange in self.exchanges_out:
            exchange.push(
                Message(
                    data={
                        "type": "data",
                        "data": message,
                        "session_id": self.current_session_id,
                    }
                )
            )

    def is_eos(self, message: Message) -> bool:
        return message["type"] == EOS

    def check_session_id(self, message: Message):
        message_session_id = message["session_id"]
        if self.current_session_id is None:
            self.current_session_id = message_session_id
        elif message_session_id != self.current_session_id:
            raise IncorrectSessionId(
                f"Session {message_session_id} is not valid. Currently serving {self.current_session_id}"
            )

    def propagate_eos(self, _eos_msg):
        for exchange in self.exchanges_out:
            exchange.broadcast(
                Message(data={"type": EOS, "session_id": self.current_session_id})
            )

    def handle_eos(self, eos_msg):
        self.propagate_eos(eos_msg)
        self.n_eos += 1

        if self.n_eos == self.producers:
            self.processed = 0
            collected_results = self.callback.collect()
            for result in collected_results or []:
                self.put_new_message_out(result)
            self.current_session_id = None

        eos_msg.ack()

    def handle_new_message(self, message: Message):
        try:
            self.check_session_id(message)

            if self.is_eos(message):
                self.handle_eos(message)
                return

            message_out = self.callback(message["data"])

            if message_out is not None:
                self.put_new_message_out(message_out)

            message.ack()
        except IncorrectSessionId:
            logger.info(
                "Dropped message due to bad session id (%s vs %s)",
                message["session_id"],
                self.current_session_id,
            )
        except Exception:
            logger.error("Unhandled exception with message %s", message, exc_info=True)
        finally:
            self.processed += 1
            if self.processed != 0 and (self.processed % 100) == 0:
                logger.info("Processed %s messages so far", self.processed)

    # TODO: Handlear sigterm con un queue.close()
    def run(self):
        self.queue_in.consume(self.handle_new_message)
