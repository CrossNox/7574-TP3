from multiprocessing import Process
import time
from typing import List

from lazarus.cfg import cfg
from lazarus.constants import DEFAULT_SLEEP_TIME


class Heartbeat(Process):
    def __init__(
        self,
        url: str,
        sleep_time: int = cfg.lazarus.sleep_time(default=DEFAULT_SLEEP_TIME),
    ):
        super().__init__()
        self.url = url
        self.sleep_time = sleep_time

    def send_heartbeat(self):
        raise NotImplementedError

    def run(self):
        while True:
            time.sleep(self.sleep_time)
            self.send_heartbeat()


class Ping(Process):
    def __init__(
        self,
        urls: List[str],
        sleep_time: int = cfg.lazarus.sleep_time(default=DEFAULT_SLEEP_TIME),
    ):
        super().__init__()
        self.urls = urls
        self.sleep_time = sleep_time

    def ping(self, url):
        raise NotImplementedError

    def run(self):
        # TODO: implement with threads
        while True:
            time.sleep(self.sleep_time)
            for url in self.urls:
                self.ping(url)
