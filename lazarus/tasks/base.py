import abc
from typing import Dict, List, Optional

ResultsList = List[Dict]


class Task(abc.ABC):
    def __init__(self):
        self.pipe_to: Optional[Task] = None

    def __rshift__(self, other):
        self.pipe_to = other
        return self

    @abc.abstractmethod
    def _execute(self, message: Dict):
        pass

    def __call__(self, message: Optional[Dict]) -> Optional[Dict]:
        if message is None:
            return None
        result = self._execute(message)
        if self.pipe_to is not None:
            return self.pipe_to(result)
        return result

    @abc.abstractmethod
    def collect(self) -> Optional[ResultsList]:
        pass
