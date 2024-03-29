import time
from multiprocessing import Event, Process
from typing import List, Tuple, Callable, Optional
from multiprocessing.synchronize import Event as EventClass

import zmq

from lazarus.cfg import cfg
from lazarus.utils import get_logger
from lazarus.constants import (
    PING,
    EPSILON,
    HEARTBEAT,
    DEFAULT_PING_PORT,
    DEFAULT_SLEEP_TIME,
    DEFAULT_HEARTBEAT_PORT,
)

logger = get_logger(__name__)


class HeartbeatSender(Process):
    def __init__(
        self,
        node_id: str,
        sleep_time: int = cfg.lazarus.sleep_time(default=DEFAULT_SLEEP_TIME, cast=int),
        port: int = cfg.lazarus.heartbeat_port(
            default=DEFAULT_HEARTBEAT_PORT, cast=int
        ),
    ):
        """Publish a heartbeat at regular intervals to notify you are still alive."""
        super().__init__()
        self.node_id = node_id
        self.sleep_time = sleep_time
        self.port = port

        self.socket: zmq.sugar.socket.Socket

    def send_heartbeat(self):
        logger.info("Sending heartbeat")
        self.socket.send_json(dict(payload=HEARTBEAT, node_id=self.node_id))

    def run(self):
        ctx = zmq.Context.instance()

        self.socket = ctx.socket(zmq.PUB)
        self.socket.bind(f"tcp://*:{self.port}")

        logger.info("Publishing heartbeats at %s", self.port)

        while True:
            time.sleep(self.sleep_time)
            self.send_heartbeat()


def monitor_heartbeat(
    host: str,
    port: int,
    error_callback: Callable,
    healthy: EventClass,
    sleep_time: int,
    tolerance: int = 3,
):
    """Monitor the heartbeat of a host.

    Args:
        host: The hostname to monitor on.
        port: The port the host is binded to.
        error_callback: Callable receiving host and port on error.
        sleep_time: Milliseconds to sleep between consecutive heartbeats.
        tolerance: How many heartbeats the host can miss before being declared gone.
    """

    logger.info("Heartbeat monitor subscribed to %s:%s", host, port)

    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    socket.RCVTIMEO = int(sleep_time * (1 + EPSILON))
    socket.connect(f"tcp://{host}:{port}")

    misses = 0
    while True:
        try:
            heartbeat = socket.recv_json()
            if not heartbeat["payload"] == HEARTBEAT or heartbeat["node_id"] != host:  # type: ignore
                raise ValueError(f"Expected heartbeat {HEARTBEAT}, got {heartbeat}")
            logger.info("Heartbeat %s from %s Ok", heartbeat, host)
            misses = 0
            if not healthy.is_set():
                healthy.set()
        except zmq.error.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                misses += 1
                logger.error("Miss %s / %s for %s:%s", misses, tolerance, host, port)
            else:
                logger.error("Unhandled ZMQ error", exc_info=True)
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
        sleep_time: int = cfg.lazarus.sleep_time(default=DEFAULT_SLEEP_TIME, cast=int),
        all_healthy: Optional[EventClass] = None,
    ):
        """Monitor the heartbeats of hosts."""
        super().__init__()
        self.sleep_time = sleep_time
        self.hosts = hosts
        self.error_callback = error_callback
        self.all_healthy = all_healthy

    def run(self):
        healthies = [Event() for _ in self.hosts]
        listeners = [
            Process(
                target=monitor_heartbeat,
                args=(
                    host,
                    port,
                    self.error_callback,
                    healthy,
                    self.sleep_time * 1000,
                ),
            )
            for (host, port), healthy in zip(self.hosts, healthies)
        ]
        logger.info("Listening to the heartbeats of %s", len(listeners))
        for p in listeners:
            p.start()
        logger.info("Waiting for all to be healthy")
        for h in healthies:
            h.wait()
        logger.info("All healthy!")
        if self.all_healthy is not None:
            self.all_healthy.set()
        for p in listeners:
            p.join()
        # TODO: catch KeyboardInterrupt and SIGTERM


class PingReplier(Process):
    def __init__(self, port: int = cfg.lazarus.ping_port(default=DEFAULT_PING_PORT)):
        """Reply to pings to notify others that you are still alive."""
        super().__init__()
        self.port = port
        self.socket = zmq.sugar.socket.Socket

    def reply_to_ping(self):
        ping = self.socket.recv_string()
        if ping != PING:
            raise ValueError(f"{ping} is not PING: {PING}")
        self.socket.send_string(PING)

    def run(self):
        ctx = zmq.Context.instance()

        self.socket = ctx.socket(zmq.REP)
        self.socket.bind("tcp://*:{self.port}")

        while True:
            self.reply_to_ping()


def monitor_ping(
    host: str, port: int, error_callback: Callable, sleep_time: int, tolerance: int = 3
):
    """Monitor ping periodically for host.

    Args:
        host: the address of the the host to ping.
        port: the port on which to ping the host.
        error_callback: the callback after _tolerance_ failed pings.
        sleep_time: milliseconds to sleep between pings. Epsilon added.
    """
    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.REQ)
    socket.RCVTIMEO = int(sleep_time * (1 + EPSILON))
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
        """Periodically send pings to monitor health of hosts.

        Args:
            hosts: list of hosts to monitor.
            error_callback: callback to handle missing nodes.
            sleep_time: seconds to sleep between pings.
        """
        super().__init__()
        self.hosts = hosts
        self.error_callback = error_callback
        self.sleep_time = sleep_time

    def run(self):
        listeners = [
            Process(
                target=monitor_ping,
                args=(host, port, self.error_callback, self.sleep_time * 1000),
            )
            for host, port in self.hosts
        ]
        for p in listeners:
            p.start()
