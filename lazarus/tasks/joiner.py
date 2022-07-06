from typing import Dict, Optional

from lazarus.tasks.base import Task


class Joiner(Task):
    def __init__(self, merge_key: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.merge_key = merge_key
        self.data: Dict[str, Dict] = {}

    def __call__(self, message: Dict) -> Optional[Dict]:
        message_key = message[self.merge_key]
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
