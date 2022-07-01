import time
from multiprocessing import Process
from typing import List, Tuple, Callable

import zmq

from lazarus.cfg import cfg
from lazarus.constants import (
    PING,
    HEARTBEAT,
    DEFAULT_PING_PORT,
    DEFAULT_SLEEP_TIME,
    DEFAULT_HEARTBEAT_PORT,
)


class HeartbeatSender(Process):
    def __init__(
        self,
        sleep_time: int = cfg.lazarus.sleep_time(default=DEFAULT_SLEEP_TIME),
        port: int = cfg.lazarus.heartbeat_port(default=DEFAULT_HEARTBEAT_PORT),
    ):
        """Publish a heartbeat at regular intervals to notify you are still alive."""
        super().__init__()
        self.sleep_time = sleep_time
        self.port = port

        ctx = zmq.Context.instance()

        self.socket = ctx.socket(zmq.PUB)
        self.socket.bind(f"tcp://*:{self.port}")

    def send_heartbeat(self):
        self.socket.send_string(HEARTBEAT)

    def run(self):
        while True:
            time.sleep(self.sleep_time)
            self.send_heartbeat()


def monitor_heartbeat(
    host: str, port: int, error_callback: Callable, sleep_time: int, tolerance: int = 3
):
    """Monitor the heartbeat of a host.

    Args:
        host: The hostname to monitor on.
        port: The port the host is binded to.
        error_callback: Callable receiving host and port on error.
        sleep_time: Time between consecutive heartbeats.
        tolerance: How many heartbeats the host can miss before being declared dead.
    """

    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    socket.setsockopt_string(zmq.RCVTIMEO, sleep_time)
    socket.connect(f"tcp://{host}:{port}")

    misses = 0
    while True:
        try:
            heartbeat = socket.recv_string()
            if not heartbeat == HEARTBEAT:
                raise ValueError(f"Expected heartbeat {HEARTBEAT}, got {heartbeat}")
        except zmq.error.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                misses += 1
            else:
                raise
        finally:
            if misses == tolerance:
                error_callback(host, port)
                misses = 0


class HeartbeatsListener(Process):
    def __init__(
        self,
        hosts: List[Tuple[str, int]],
        error_callback: Callable,
        sleep_time: int = cfg.lazarus.sleep_time(default=DEFAULT_SLEEP_TIME),
    ):
        """Monitor the heartbeats of hosts."""
        super().__init__()
        self.sleep_time = sleep_time
        self.hosts = hosts
        self.error_callback = error_callback

    def run(self):
        listeners = [
            Process(
                target=monitor_heartbeat,
                args=(host, port, self.error_callback, self.sleep_time),
            )
            for host, port in self.hosts
        ]
        for p in listeners:
            p.start()
        # TODO: catch KeyboardInterrupt and SIGTERM


class PingReplier(Process):
    def __init__(self, port: int = cfg.lazarus.ping_port(default=DEFAULT_PING_PORT)):
        """Reply to pings to notify others that you are still alive."""
        super().__init__()
        self.port = port

        ctx = zmq.Context.instance()

        self.socket = ctx.socket(zmq.REP)
        self.socket.bind("tcp://*:{self.port}")

    def reply_to_ping(self):
        ping = self.socket.recv_string()
        if ping != PING:
            raise ValueError(f"{ping} is not PING: {PING}")
        self.socket.send_string(PING)

    def run(self):
        while True:
            self.reply_to_ping()


def monitor_ping(
    host: str, port: int, error_callback: Callable, sleep_time: int, tolerance: int = 3
):

    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.REQ)
    socket.setsockopt_string(zmq.RCVTIMEO, sleep_time)
    socket.connect(f"tcp://{host}:{port}")

    misses = 0
    while True:
        try:
            time.sleep(sleep_time)
            socket.send_string(PING)
            reply = socket.recv_string()
            if not reply == PING:
                raise ValueError(f"Expected ping {PING}, but got {reply}")
        except zmq.error.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                misses += 1
            else:
                raise
        finally:
            if misses == tolerance:
                error_callback(host, port)
                misses = 0


class PingMonitor(Process):
    def __init__(
        self,
        hosts: List[Tuple[str, int]],
        error_callback: Callable,
        sleep_time: int = cfg.lazarus.sleep_time(default=DEFAULT_SLEEP_TIME),
    ):
        """Periodically send pings to monitor health of hosts."""
        super().__init__()
        self.hosts = hosts
        self.error_callback = error_callback
        self.sleep_time = sleep_time

    def run(self):
        listeners = [
            Process(
                target=monitor_ping,
                args=(host, port, self.error_callback, self.sleep_time),
            )
            for host, port in self.hosts
        ]
        for p in listeners:
            p.start()
