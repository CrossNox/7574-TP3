import abc
from typing import Dict, List, Optional

ResultsList = List[Dict]


class Task(abc.ABC):
    def __init__(self):
        self.pipe_to: Optional[Task] = None

    def set_downstream(self, other):
        self.pipe_to = other

    def __rshift__(self, other):
        """Compose tasks easily.

        Example:
        ```python
            a1 = Task()
            a2 = Task()
            a3 = Task()
            a1 >> a2 >> a3
        ```
        """
        self.set_downstream(other)
        return other

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

    def reset(self):
        pass
