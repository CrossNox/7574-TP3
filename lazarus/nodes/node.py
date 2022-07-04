import threading
from multiprocessing import Process
from typing import Dict, Type, TypeVar, Optional, Sequence

from lazarus.constants import EOS
from lazarus.mom.queue import Queue
from lazarus.tasks.base import Task
from lazarus.utils import get_logger
from lazarus.mom.message import Message
from lazarus.tasks.joiner import Joiner
from lazarus.mom.exchange import Exchange
from lazarus.exceptions import IncorrectSessionId

logger = get_logger(__name__)

TaskCls = TypeVar("TaskCls", bound=Task)


class Node(Process):
    def __init__(
        self,
        callback: Type[TaskCls],
        queue_in: Queue,
        exchanges_out: Sequence[Exchange],
        producers: int = 1,
        dependencies: Optional[Dict[str, Queue]] = None,
        **callback_kwargs,
    ):
        super().__init__()
        self.current_session_id: Optional[str] = None
        self.producers = producers
        self.n_eos = 0
        self.queue_in = queue_in
        self.exchanges_out = exchanges_out
        self.processed = 0
        logger.info("Solving dependencies")
        self.dependencies = self.wait_for_dependencies(dependencies or {})
        logger.info("Solved dependencies %s", self.dependencies)
        self.callback = callback(**self.dependencies, **callback_kwargs)

    def wait_for_dependencies(self, dependencies):
        solved_dependencies = {}
        for dependency, queue in dependencies.items():
            logger.info("Solving %s from %s", dependency, queue)
            solved_dependencies[dependency] = self.fetch_result(dependency, queue)
        return solved_dependencies

    def fetch_result(self, key: str, queue: Queue):
        done_event = threading.Event()

        class DummyCallback:
            def __init__(self, done, key):
                self.result = None
                self.key = key
                self.done = done

            def __call__(self, message):
                logger.error("Dependency callback :: message %s", message)
                if message["type"] != EOS:
                    self.result = message["data"][self.key]
                else:
                    self.done.set()
                message.ack()

        _callback = DummyCallback(done_event, key)

        queue.consume(_callback)
        done_event.wait()
        queue.close()

        return _callback.result

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

    def propagate_eos(self):
        logger.info("Propagating EOS")
        for exchange in self.exchanges_out:
            exchange.broadcast(
                Message(data={"type": EOS, "session_id": self.current_session_id})
            )

    def handle_eos(self, eos_msg):
        self.n_eos += 1

        logger.info("Received %s/%s EOS", self.n_eos, self.producers)
        if self.n_eos == self.producers:
            self.processed = 0
            collected_results = self.callback.collect() or []
            logger.info("Collected %s results", len(collected_results))

            for result in collected_results:
                self.put_new_message_out(result)

            self.propagate_eos()

            self.current_session_id = None
            self.n_eos = 0

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


class QueueConsumer:
    def __init__(
        self,
        producers,
        queue,
        callback,
        exchanges_out,
        session_id=None,
        propagate_eos=True,
    ):
        self.producers = producers
        self.queue_in = queue
        self.callback = callback
        self.exchanges_out = exchanges_out
        self.session_id = session_id
        self.propagate_eos = propagate_eos
        self.done_event = threading.Event()
        self.n_eos = 0

    def __put_new_message_out(self, message: Dict) -> None:
        # TODO: decorate message with session id and type (data?)
        for exchange in self.exchanges_out:
            exchange.push(
                Message(
                    data={
                        "type": "data",
                        "data": message,
                        "session_id": self.session_id,
                    }
                )
            )

    def __is_eos(self, message: Message) -> bool:
        return message["type"] == EOS

    def __check_session_id(self, message: Message):
        message_session_id = message["session_id"]
        if self.session_id is None:
            self.session_id = message_session_id
        elif message_session_id != self.session_id:
            raise IncorrectSessionId(
                f"Session {message_session_id} is not valid. Currently serving {self.session_id}"
            )

    def __propagate_eos(self):
        logger.info("Propagating EOS")
        for exchange in self.exchanges_out:
            exchange.broadcast(
                Message(data={"type": EOS, "session_id": self.session_id})
            )

    def __should_finish(self, eos_msg: Message) -> bool:
        self.n_eos += 1
        if self.n_eos == self.producers:
            if self.propagate_eos:
                self.__propagate_eos()
            eos_msg.ack()
            return True
        eos_msg.ack()
        return False

    def __handle_msg(self, message: Message):
        try:
            self.__check_session_id(message)

            if self.__is_eos(message):
                if self.__should_finish(message):
                    self.done_event.set()
                return

            message_out = self.callback(message["data"])

            if message_out is not None:
                self.__put_new_message_out(message_out)

            message.ack()

        except IncorrectSessionId:
            logger.info(
                "Dropped message due to bad session id (%s vs %s)",
                message["session_id"],
                self.session_id,
            )
        except Exception:
            logger.error("Unhandled exception with message %s", message, exc_info=True)

    def consume(self):
        self.queue_in.consume(self.__handle_msg)
        self.done_event.wait()
        self.queue_in.close()

        return self.session_id


class JoinNode(Process):
    def __init__(
        self,
        joiner: Type[Joiner],
        add_queue: Queue,
        join_queue: Queue,
        exchanges_out: Sequence[Exchange],
        add_producers: int = 1,
        join_producers: int = 1,
    ):
        super().__init__()
        self.current_session_id: Optional[str] = None
        self.joiner = joiner
        self.add_queue = add_queue
        self.join_queue = join_queue
        self.exchanges_out = exchanges_out
        self.add_producers = add_producers
        self.join_producers = join_producers

    def run(self):
        wait_forever = threading.Event()
        while True:
            add_consumer = QueueConsumer(
                self.add_producers,
                self.add_queue,
                self.joiner.add,
                [],
                propagate_eos=False,
            )

            logger.info("Start consuming add queue")
            session_id = add_consumer.consume()

            join_consumer = QueueConsumer(
                self.join_producers,
                self.join_queue,
                self.joiner.join,
                self.exchanges_out,
                session_id=session_id,
                propagate_eos=True,
            )

            logger.info("Start consuming join queue")
            join_consumer.consume()

            logger.info("Reset joiner")
            self.joiner.reset()

            wait_forever.wait()
