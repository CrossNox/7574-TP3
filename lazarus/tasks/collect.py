from lazarus.tasks.base import Task


class Collector(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue_messages = {}

    def __call__(self, message, queue_name):
        if queue_name not in self.queue_messages:
            self.queue_messages[queue_name] = []
        self.queue_messages[queue_name].append(message)

    def collect(self):
        return [self.queue_messages]
