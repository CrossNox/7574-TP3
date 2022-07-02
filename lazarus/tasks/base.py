import abc
from typing import Dict, List, Optional

ResultsList = List[Dict]


class Task(abc.ABC):
    @abc.abstractmethod
    def __call__(self, message: Dict) -> Optional[Dict]:
        pass

    @abc.abstractmethod
    def collect(self) -> Optional[ResultsList]:
        pass
