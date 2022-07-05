import threading
from threading import Lock
from multiprocessing import Process
from typing import Dict, List, Type, Union, TypeVar, Optional, Sequence

from lazarus.constants import EOS
from lazarus.mom.queue import Queue
from lazarus.tasks.base import Task
from lazarus.utils import get_logger
from lazarus.mom.message import Message
from lazarus.mom.exchange import Exchange
from lazarus.exceptions import IncorrectSessionId

logger = get_logger(__name__)

TaskCls = TypeVar("TaskCls", bound=Task)


class Node(Process):
    def __init__(
        self,
        callback: Type[TaskCls],
        queue_in: Union[List[Queue], Queue],
        exchanges_out: Sequence[Exchange],
        producers: Union[List[int], int] = 1,
        dependencies: Optional[Dict[str, Queue]] = None,
        **callback_kwargs,
    ):
        super().__init__()
        self.current_session_id: Optional[str] = None

        self.producers: List[int]
        if isinstance(producers, list):
            self.producers = producers
        else:
            self.producers = [producers]

        self.queues_in: List[Queue]
        if isinstance(queue_in, list):
            self.queues_in = queue_in
        else:
            self.queues_in = [queue_in]

        self.n_eos = {q.queue_name: p for q, p in zip(self.queues_in, self.producers)}
        self.received_eos = {q.queue_name: 0 for q in self.queues_in}

        self.exchanges_out = exchanges_out
        self.processed = 0

        self.callback_cls = callback
        self.callback_kwargs = callback_kwargs

        self.dependencies_raw = dependencies or {}
        logger.info("Solving dependencies")
        self.dependencies = self.wait_for_dependencies(self.dependencies_raw)
        logger.info("Solved dependencies %s", self.dependencies)
        self.callback = self.callback_cls(**self.dependencies, **self.callback_kwargs)

        self.run_lock = Lock()

    def wait_for_dependencies(self, dependencies):
        solved_dependencies = {}
        # TODO: paralelizar
        for dependency, queue in dependencies.items():
            logger.info("Solving %s from %s", dependency, queue)
            solved_dependencies[dependency] = self.fetch_result(dependency, queue)
        return solved_dependencies

    def fetch_result(self, key: str, queue: Queue):
        done_event = threading.Event()

        class DummyCallback:
            def __init__(self, done, key, n_eos: int = 1):
                self.result = None
                self.key = key
                self.n_eos = n_eos
                self.done = done

            def __call__(self, message):
                logger.error("Dependency callback :: message %s", message)
                if message["type"] != EOS:
                    self.result = message["data"][self.key]
                else:
                    self.n_eos -= 1
                    if self.n_eos == 0:
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

            self.received_eos = {q.queue_name: 0 for q in self.queues_in}

            logger.info("Solving dependencies")
            self.dependencies = self.wait_for_dependencies(self.dependencies_raw)
            logger.info("Solved dependencies %s", self.dependencies)

            self.callback = self.callback_cls(
                **self.dependencies, **self.callback_kwargs
            )
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

    def handle_eos(self, eos_msg, queue_name):
        self.received_eos[queue_name] += 1

        logger.info("Received %s/%s EOS", self.received_eos, self.n_eos)
        if all(self.received_eos[k] == v for k, v in self.n_eos.values()):
            self.processed = 0
            collected_results = self.callback.collect() or []
            logger.info("Collected %s results", len(collected_results))

            for result in collected_results:
                self.put_new_message_out(result)

            self.propagate_eos()

            self.current_session_id = None
            self.n_eos = 0

        eos_msg.ack()

    def handle_new_message(self, message: Message, queue_name: str):
        self.run_lock.acquire()
        try:
            self.check_session_id(message)

            if self.is_eos(message):
                self.handle_eos(message, queue_name)
                self.run_lock.release()
                return

            message_out = self.callback(message["data"])

            if message_out is not None:
                self.put_new_message_out(message_out)

            message.ack()
            self.run_lock.release()

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
        class DummyQueueCallback:
            # TODO
            # 1. Either move this out of here
            # 2. Use a partial function
            def __init__(self, queue_name, f):
                self.queue_name = queue_name
                self.f = f

            def __call__(self, message: Message):
                return self.f(message, queue_name=self.queue_name)

        for q in self.queues_in:
            queue_callback = DummyQueueCallback(q.queue_name, self.handle_new_message)
            q.consume(queue_callback)
