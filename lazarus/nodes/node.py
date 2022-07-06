import threading
from multiprocessing import Process
from typing import Dict, List, Type, Union, TypeVar, Optional, Sequence

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from lazarus.constants import EOS
from lazarus.mom.queue import Queue
from lazarus.tasks.base import Task
from lazarus.utils import get_logger
from lazarus.mom.message import Message
from lazarus.mom.exchange import Exchange
from lazarus.storage.base import BaseStorage
from lazarus.exceptions import IncorrectSessionId

logger = get_logger(__name__)

TaskCls = TypeVar("TaskCls", bound=Task)


class Node(Process):
    def __init__(
        self,
        identifier: str,
        callback: Type[TaskCls],
        queue_in: Union[List[Queue], Queue],
        exchanges_out: Sequence[Exchange],
        producers: Union[List[int], int] = 1,
        dependencies: Optional[Dict[str, Queue]] = None,
        storage: Optional[BaseStorage] = None,
        **callback_kwargs,
    ):
        super().__init__()
        self.identifier = identifier
        self.current_session_id: Optional[str] = None

        self.storage = storage

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
        self.dependencies = self.solve_dependencies()
        logger.info("Solved dependencies %s", self.dependencies)
        self.callback = self.callback_cls(**self.dependencies, **self.callback_kwargs)

        self.run_lock = threading.Lock()

        # If there are any messages, and we need to recover, reprocess each message
        if self.storage is not None and self.storage.contains_topic("messages"):
            logger.info("Reprocessing messages from storage")
            with self.storage.recovery_mode(), logging_redirect_tqdm():
                for _, v in tqdm(
                    iterable=self.storage.iter_topic("messages"),
                    desc="Recovery going on",
                ):
                    message_origin_queue = v.pop("origin_queue")
                    self.handle_new_message(
                        Message(data=v), queue_name=message_origin_queue
                    )
            logger.info("Reprocessing messages from storage done")

        logger.info("Initialization done")

    def solve_dependencies(self):
        dependencies = self.dependencies_raw
        try:
            logger.info("Trying to load dependencies from storage")
            return {k: v for k, v in self.storage.iter_topic("dependencies")}
        except (AttributeError, KeyError):
            logger.info("Loading dependencies from storage failed")

        solved_dependencies = {}
        # TODO: paralelizar
        for dependency, queue in dependencies.items():
            logger.info("Solving %s from %s", dependency, queue)
            dependency_value = self.fetch_result(dependency, queue)
            solved_dependencies[dependency] = dependency_value

            if self.storage is not None:
                self.storage.put(dependency, dependency_value, topic="dependencies")

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

    def put_new_message_out(self, message: Dict, **kwargs) -> None:
        # TODO: decorate message with session id and type (data?)
        if self.storage is not None and self.storage.in_recovery_mode:
            return

        for exchange in self.exchanges_out:
            exchange.push(
                Message(
                    data={
                        "type": "data",
                        "data": message,
                        "session_id": self.current_session_id,
                        **kwargs,
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
            self.dependencies = self.solve_dependencies()
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
                Message(
                    data={
                        "type": EOS,
                        "session_id": self.current_session_id,
                        "id": self.identifier,
                    }
                )
            )

    def handle_eos(self, _eos_msg, queue_name):
        # TODO: save the identifier instead of just adding 1
        self.received_eos[queue_name] += 1

        logger.info("Received %s/%s EOS", self.received_eos, self.n_eos)
        if all(self.received_eos[k] == v for k, v in self.n_eos.items()):
            self.processed = 0
            collected_results = self.callback.collect() or []
            logger.info("Collected %s results", len(collected_results))

            for result in collected_results:
                self.put_new_message_out(result)

            self.propagate_eos()

            self.current_session_id = None
            self.n_eos = 0

    def handle_new_message(self, message: Message, queue_name: str):
        self.run_lock.acquire()
        try:
            message_id = message.get("id") or message.get("data", {}).get("id")
            if message_id is None:
                raise ValueError("Id can't be found")

            if self.storage is not None and not self.storage.in_recovery_mode:
                # Have I seen this message for this session id?
                # Drop duplicates
                message_session_id = message["session_id"]
                message_type = message["type"]
                message_id = f"{message_session_id}_{message_type}_{message_id}"

                if self.storage.contains(message_id, topic="messages"):
                    # Drop duplicates
                    logger.info(
                        "Message %s already seen (present in storage). Dropping.",
                        message,
                    )
                    return

            if self.storage is not None:
                self.storage.put(
                    message_id,
                    {**message.data, "origin_queue": queue_name},
                    topic="messages",
                )

            self.check_session_id(message)

            if self.is_eos(message):
                self.handle_eos(message, queue_name)
                return

            message_out = self.callback(message["data"])

            if message_out is not None:
                self.put_new_message_out(message_out)

        except IncorrectSessionId:
            logger.info(
                "Dropped message due to bad session id (%s vs %s)",
                message["session_id"],
                self.current_session_id,
            )
        except Exception:
            logger.error("Unhandled exception with message %s", message, exc_info=True)
        finally:
            try:
                message.ack()
            except NotImplementedError:
                if self.storage is not None and self.storage.in_recovery_mode:
                    pass
                else:
                    raise

            self.processed += 1
            if self.processed != 0 and (self.processed % 100) == 0:
                logger.info("Processed %s messages so far", self.processed)

            self.run_lock.release()

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
