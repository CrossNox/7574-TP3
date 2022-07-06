import os
from threading import Thread

import zmq

from lazarus.cfg import cfg
from lazarus.utils import get_logger
from lazarus.constants import (
    PING,
    VICTORY,
    ELECTION,
    BULLY_TIMEOUT_MS,
    DEFAULT_BULLY_PORT,
    DEFAULT_BULLY_TOLERANCE,
)

logger = get_logger(__name__)


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
    container,
    group,
    tolerance: int = cfg.lazarus.bully_tolerance(
        default=DEFAULT_BULLY_TOLERANCE, cast=int
    ),
):
    logger.info("%s starting leader election for size-%d group", container, len(group))
    os.environ["LAZARUS_GROUP_LEADER"] = ""

    ctx = zmq.Context.instance()

    election_notified = []
    for sibling in [c for c in group if c > container]:
        socket = ctx.socket(zmq.REQ)
        socket.RCVTIMEO = BULLY_TIMEOUT_MS
        socket.SNDTIMEO = BULLY_TIMEOUT_MS
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
            socket.RCVTIMEO = BULLY_TIMEOUT_MS
            socket.SNDTIMEO = BULLY_TIMEOUT_MS
            socket.connect(f"tcp://{sibling}:{DEFAULT_BULLY_PORT}")
            msg = {"type": VICTORY, "host": container}
            send = try_send(container, sibling, socket, msg, tolerance)
            if not send:
                continue
            ping = try_recv(container, sibling, socket, "PING", tolerance)
            if ping:
                leader_notified.append(sibling)

        logger.info("%s is the leader", container)
        os.environ["LAZARUS_GROUP_LEADER"] = container
        return


class LeaderElectionListener(Thread):
    def __init__(self, port: int = cfg.lazarus.bully_port(default=DEFAULT_BULLY_PORT)):
        super().__init__()
        self.port = port
        self.tolerance = cfg.lazarus.bully_tolerance(
            default=DEFAULT_BULLY_TOLERANCE, cast=int
        )
        self.identifier = cfg.lazarus.identifier()
        ctx = zmq.Context.instance()

        self.socket = ctx.socket(zmq.REP)
        self.socket.RCVTIMEO = BULLY_TIMEOUT_MS
        self.socket.SNDTIMEO = BULLY_TIMEOUT_MS
        self.socket.bind(f"tcp://*:{self.port}")

    def reply_to_leader_election(self):
        response = self.socket.recv_json()
        logger.info("%s got %s", self.identifier, response["type"])

        ping_msg = {"type": PING, "host": self.identifier}
        if response["type"] == ELECTION:
            logger.info("Received ELECTION")
            try_send(self.identifier, "sibling", self.socket, ping_msg, self.tolerance)
            elect_leader(self.identifier, cfg.lazarus.groupids().split())
        elif response["type"] == VICTORY:
            logger.info("Received VICTORY")
            try_send(self.identifier, "sibling", self.socket, ping_msg, self.tolerance)
            os.environ["LAZARUS_GROUP_LEADER"] = response["host"]

    def run(self):
        while True:
            logger.info("Listening for leader election")
            self.reply_to_leader_election()


def get_leader():
    return os.environ.get("LAZARUS_GROUP_LEADER")


def is_leader():
    return get_leader() == cfg.lazarus.identifier()
