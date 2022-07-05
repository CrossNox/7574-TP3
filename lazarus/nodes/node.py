from multiprocessing import Process
import threading
from typing import Dict, Type, TypeVar, Optional, Sequence

from tqdm import tqdm

from lazarus.constants import EOS
from lazarus.exceptions import IncorrectSessionId
from lazarus.mom.exchange import Exchange
from lazarus.mom.message import Message
from lazarus.mom.queue import Queue
from lazarus.storage.base import BaseStorage
from lazarus.tasks.base import Task
from lazarus.utils import get_logger

logger = get_logger(__name__)

TaskCls = TypeVar("TaskCls", bound=Task)


class Node(Process):
    def __init__(
        self,
        identifier: str,
        callback: Type[TaskCls],
        queue_in: Queue,
        exchanges_out: Sequence[Exchange],
        producers: int = 1,
        dependencies: Optional[Dict[str, Queue]] = None,
        storage: Optional[BaseStorage] = None,
        **callback_kwargs,
    ):
        super().__init__()
        self.identifier = identifier
        self.current_session_id: Optional[str] = None
        self.producers = producers
        self.n_eos = 0
        self.queue_in = queue_in
        self.exchanges_out = exchanges_out
        self.processed = 0
        self.storage = storage

        logger.info("Solving dependencies")
        self.dependencies = self.solve_dependencies(dependencies or {})
        logger.info("Solved dependencies %s", self.dependencies)

        # Now we can instantiate the callback
        self.callback = callback(**self.dependencies, **callback_kwargs)

        # If there are any messages, and we need to recover, reprocess each message
        if self.storage is not None and self.storage.contains_topic("messages"):
            logger.info("Reprocessing messages from storage")
            with self.storage.recovery_mode():
                for _, v in tqdm(
                    iterable=self.storage.iter_topic("messages"),
                    desc="Recovery going on",
                ):
                    self.handle_new_message(Message(data=v))
            logger.info("Reprocessing messages from storage done")

    def solve_dependencies(self, dependencies):
        try:
            logger.info("Trying to load dependencies from storage")
            return {k: v for k, v in self.storage.iter_topic("dependencies")}
        except (AttributeError, KeyError):
            logger.info("Loading dependencies from storage failed")

        solved_dependencies = {}
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
                Message(
                    data={
                        "type": EOS,
                        "session_id": self.current_session_id,
                        "id": self.identifier,
                    }
                )
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
        try:
            eos_msg.ack()
        except NotImplementedError:
            if self.storage is not None and self.storage.in_recovery_mode:
                pass
            else:
                raise

    def handle_new_message(self, message: Message):
        try:
            if self.storage is not None:
                # Have I seen this message for this session id?
                message_id = message.get("id") or message.get("data", {}).get("id")
                if message_id is None:
                    raise ValueError("Id can't be found")
                message_session_id = message["session_id"]
                message_type = message["type"]
                message_id = f"{message_session_id}_{message_type}_{message_id}"

                if self.storage.contains(message_id, topic="messages"):
                    # Drop duplicates
                    logger.info(
                        "Message %s already seen (present in storage). Dropping.",
                        message,
                    )

                    try:
                        # Duplicate messages require no processing
                        message.ack()
                        return
                    except NotImplementedError:
                        if self.storage is not None and self.storage.in_recovery_mode:
                            return
                        else:
                            raise

            self.check_session_id(message)
            self.storage.put(message_id, message.data, topic="messages")

            if self.is_eos(message):
                self.handle_eos(message)
                return

            message_out = self.callback(message["data"])

            if message_out is not None:
                self.put_new_message_out(message_out)

            try:
                message.ack()
            except NotImplementedError:
                if self.storage is not None and self.storage.in_recovery_mode:
                    pass
                else:
                    raise

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
