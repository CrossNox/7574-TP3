import time
from typing import Any, Dict, List
from multiprocessing import Process
from multiprocessing.sharedctypes import Synchronized

import zmq

from lazarus.cfg import cfg
from lazarus.utils import get_logger
from lazarus.constants import (
    PING,
    UNKNOWN,
    VICTORY,
    ELECTION,
    LOOKINGFOR,
    BULLY_TIMEOUT_MS,
    DEFAULT_BULLY_PORT,
    DEFAULT_BULLY_TOLERANCE,
)

logger = get_logger(__name__)


def wait_for_leader(leader_value: Synchronized):
    logger.debug("Entering wait_for_leader")
    while True:
        logger.info("wait_for_leader:: Acquiring log")
        logger.info("wait_for_leader:: Lock acquired")
        keep_going = leader_value.value in (UNKNOWN, LOOKINGFOR)
        logger.info("wait_for_leader:: Lock released")
        if keep_going:
            logger.info("wait_for_leader:: keep going, sleeping")
            time.sleep(cfg.lazarus.bully_timeout(default=BULLY_TIMEOUT_MS, cast=int))
        else:
            break
    logger.debug("Leaving wait_for_leader")


def get_leader(leader_value):
    leader = get_leader_state(leader_value)
    return None if leader in (UNKNOWN, LOOKINGFOR) else leader


def am_leader(leader_value):
    return get_leader(leader_value) == cfg.lazarus.identifier()


def get_leader_state(leader_value) -> str:
    logger.info("Entering get_leader_state")
    leader = leader_value.value
    logger.info("Leaving get_leader_state w/leader %s", leader)
    return leader


def try_send(container, sibling, socket, msg, tolerance):
    for i in range(tolerance):
        try:
            socket.send_json(msg)
            logger.info("%s sending %s to %s", container, msg["type"], sibling)
            return True
        except zmq.error.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                logger.info("%s seems to be down (%d/%d)", sibling, i, tolerance - 1)
                continue
            else:
                raise
    return False


def try_recv(container, sibling, socket, expected_type, tolerance):
    for i in range(tolerance):
        try:
            reply = socket.recv_json()
            if not reply["type"] == expected_type:
                raise ValueError(f"Expected {expected_type}, but got {reply}")
            logger.info("%s got %s from %s", container, expected_type, sibling)
            return True
        except zmq.error.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                logger.info("%s seems to be down (%d/%d)", sibling, i, tolerance - 1)
                continue
            else:
                raise
    return False


def elect_leader(
    container: str,
    group: List[str],
    leader_value: Synchronized,
    tolerance: int = cfg.lazarus.bully_tolerance(
        default=DEFAULT_BULLY_TOLERANCE, cast=int
    ),
):
    if leader_value.value == LOOKINGFOR:
        logger.info("Asked for a new leader election, but one is already running!")
        return  # We are already on a election

    leader_value.value = LOOKINGFOR
    logger.info("%s starting leader election for size-%d group", container, len(group))

    ctx = zmq.Context.instance()

    election_notified = []
    for sibling in [c for c in group if c > container]:
        socket = ctx.socket(zmq.REQ)
        socket.RCVTIMEO = int(BULLY_TIMEOUT_MS * 1.1)
        socket.SNDTIMEO = int(BULLY_TIMEOUT_MS * 1.1)
        logger.info("Connect to tcp://%s:%s", sibling, DEFAULT_BULLY_PORT)
        socket.connect(f"tcp://{sibling}:{DEFAULT_BULLY_PORT}")
        msg = {"type": ELECTION, "host": container}
        send = try_send(container, sibling, socket, msg, tolerance)
        if not send:
            continue
        ping = try_recv(container, sibling, socket, "PING", tolerance)
        if send and ping:
            election_notified.append(sibling)

    leader_notified = []
    if not election_notified:
        for sibling in [c for c in group if c != container]:
            socket = ctx.socket(zmq.REQ)
            socket.RCVTIMEO = int(BULLY_TIMEOUT_MS * 1.1)
            socket.SNDTIMEO = int(BULLY_TIMEOUT_MS * 1.1)
            logger.info("Connect to tcp://%s:%s", sibling, DEFAULT_BULLY_PORT)
            socket.connect(f"tcp://{sibling}:{DEFAULT_BULLY_PORT}")
            msg = {"type": VICTORY, "host": container}
            send = try_send(container, sibling, socket, msg, tolerance)
            if not send:
                continue
            ping = try_recv(container, sibling, socket, "PING", tolerance)
            if ping:
                leader_notified.append(sibling)

        logger.info("%s is the leader", container)
        leader_value.value = container
        return


class LeaderElectionListener(Process):
    def __init__(
        self,
        node_id: str,
        group: List[str],
        leader_value: Synchronized,
        port: int = cfg.lazarus.bully_port(default=DEFAULT_BULLY_PORT),
    ):
        super().__init__()
        self.identifier = node_id
        self.group = group
        self.leader_value = leader_value
        self.port = port
        self.tolerance = cfg.lazarus.bully_tolerance(
            default=DEFAULT_BULLY_TOLERANCE, cast=int
        )
        ctx = zmq.Context.instance()

        self.socket = ctx.socket(zmq.REP)
        self.socket.RCVTIMEO = int(BULLY_TIMEOUT_MS * 1.1)
        self.socket.SNDTIMEO = int(BULLY_TIMEOUT_MS * 1.1)

        logger.info("Binding to tcp://*:%s", self.port)
        self.socket.bind(f"tcp://*:{self.port}")

        logger.info("LeaderElectionListener :: %s -> %s", self.identifier, self.group)

    def reply_to_leader_election(self) -> None:
        try:
            response: Dict[str, Any] = self.socket.recv_json()  # type:ignore
        except zmq.error.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                return
            else:
                raise

        logger.info("%s got %s", self.identifier, response["type"])

        ping_msg = {"type": PING, "host": self.identifier}
        if response["type"] == ELECTION:
            logger.info("Received ELECTION")
            try_send(self.identifier, "sibling", self.socket, ping_msg, self.tolerance)
            if get_leader_state(self.leader_value) == LOOKINGFOR:
                logger.info("I'm already in election, so we ignore this msg")
            else:
                # TODO: We are not joining processes right here
                worker = Process(
                    target=elect_leader, args=(self.identifier, self.group)
                )
                worker.start()

        elif response["type"] == VICTORY:
            logger.info("Received VICTORY")
            try_send(self.identifier, "sibling", self.socket, ping_msg, self.tolerance)
            self.leader_value = response["host"]

    def run(self):
        while True:
            logger.info("Listening for leader election")
            self.reply_to_leader_election()
            logger.info("Leader is < %s >", get_leader_state(self.leader_value))
