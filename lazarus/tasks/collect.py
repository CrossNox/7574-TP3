from typing import Dict, List

from lazarus.tasks.base import Task
from lazarus.utils import get_logger

logger = get_logger(__name__)


class Collector(Task):
    def __init__(self, keep: Dict[str, str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.keep = keep
        self.queue_messages: Dict[str, List] = {}

    def __call__(self, message, queue_name):
        if queue_name not in self.queue_messages:
            self.queue_messages[queue_name] = []

        key = self.keep[queue_name]

        if key == "*":
            self.queue_messages[queue_name].append(message)
        else:
            self.queue_messages[queue_name].append(message[key])

    def collect(self):
        logger.debug("Collector :: %s", self.queue_messages)
        final_data = {}
        for k, v in self.queue_messages.items():
            k = k.split("::")[0]
            if len(v) == 1:
                final_data[k] = v[0]
            else:
                final_data[k] = v
        logger.debug("Collector :: %s", final_data)
        return [final_data]
