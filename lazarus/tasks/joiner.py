from typing import Dict, Optional

from lazarus.tasks.base import Task
from lazarus.utils import get_logger

logger = get_logger(__name__)


class Joiner(Task):
    def __init__(self, **kwargs):
        super().__init__()
        self.merge_keys = kwargs
        logger.info("Joiner with merge_keys %s", self.merge_keys)
        self.data: Dict[str, Dict] = {}

    def __call__(self, message: Dict, queue_name: str) -> Optional[Dict]:
        logger.info(
            "Got message %s, from q %s", message, queue_name,
        )
        message_key = message[self.merge_keys[queue_name]]
        if message_key not in self.data:
            self.data[message_key] = message
            return None
        else:
            other_message = self.data.pop(message_key)
            return {**message, **other_message}

    def collect(self):
        return None

    def reset(self):
        self.data = {}
