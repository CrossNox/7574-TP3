from multiprocessing import Process
from typing import Dict, Type, TypeVar, Optional

from lazarus.constants import EOS
from lazarus.tasks.base import Task
from lazarus.exceptions import IncorrectSessionId

DependencyName = str
Host = str

TaskCls = TypeVar("TaskCls", bound=Task)


class Node(Process):
    def __init__(
        self,
        callback: Type[TaskCls],
        dependencies: Optional[Dict[DependencyName, Host]] = None,
        **callback_kwargs,
    ):
        super().__init__()
        self.current_session_id: Optional[str] = None
        self.dependencies = self.wait_for_dependencies(dependencies or {})
        self.callback = callback(**self.dependencies, **callback_kwargs)

    def wait_for_dependencies(self, dependencies):
        solved_dependencies = {}
        for dependency, host in dependencies.items():
            solved_dependencies[dependency] = self.fetch_result(host)
        return solved_dependencies

    def fetch_result(self, host):
        # TODO: actually implement this
        return host

    def get_new_message(self) -> Dict:
        return {"type": "comment", "payload": "test"}

    def put_new_message(self, message: Dict) -> None:
        pass

    def is_eos(self, message) -> bool:
        return message == EOS

    def check_session_id(self, message):
        message_session_id = message["session_id"]
        if self.current_session_id is None:
            self.current_session_id = message_session_id
        elif message_session_id != self.current_session_id:
            raise IncorrectSessionId(
                f"Session {message_session_id} is not valid. Currently serving {self.current_session_id}"
            )

    def propagate_eos(self, eos_msg):
        pass

    def handle_eos(self, eos_msg):
        self.current_session_id = None
        self.propagate_eos(eos_msg)
        collected_results = self.callback.collect()
        for result in collected_results:
            self.put_new_message(result)

    def ack_mesage(self):
        pass

    def run(self):
        while True:
            try:
                message_in = self.get_new_message()
                self.check_session_id(message_in)

                if self.is_eos(message_in):
                    self.handle_eos(message_in)
                    continue

                message_out = self.callback(message_in)

                if message_out is not None:
                    self.put_new_message(message_out)
                    self.ack_mesage()

            except IncorrectSessionId:
                # Drop message
                pass
