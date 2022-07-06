from typing import Dict

from lazarus.tasks.base import Task
from lazarus.utils import get_logger

logger = get_logger(__name__)


class Collector(Task):
    def __init__(self, keep: Dict[str, str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.keep = keep
        self.queue_messages = {}

    def __call__(self, message, queue_name):
        if queue_name not in self.queue_messages:
            self.queue_messages[queue_name] = []
        logger.debug("Collector :: (%s) - %s", queue_name, message)
        key = self.keep[queue_name]
        if key == "*":
            self.queue_messages[queue_name].append(message)
        else:
            self.queue_messages[queue_name].append({key: message[key]})

    def collect(self):
        logger.debug("Collector :: %s", self.queue_messages)
        return [self.queue_messages]
