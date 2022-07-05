import os
from threading import Thread

import zmq

from lazarus.cfg import cfg
from lazarus.utils import get_logger
from lazarus.constants import PING, VICTORY, ELECTION, DEFAULT_BULLY_PORT

logger = get_logger(__name__)


def elect_leader(container, group):
    logger.info("%s starting leader election for size-%d group", container, len(group))

    ctx = zmq.Context.instance()
    older_siblings = [c for c in group if c > container]

    pings = []
    for sibling in older_siblings:
        socket = ctx.socket(zmq.REQ)
        socket.connect(f"tcp://{sibling}:{DEFAULT_BULLY_PORT}")
        logger.info("%s sending ELECTION to %s", container, sibling)
        socket.send_json({"type": ELECTION, "host": container})

        response = socket.recv_json()
        if response["type"] == PING:
            logger.info("%s got PING from %s", container, sibling)
            pings.append(sibling)

    if not pings or not older_siblings:
        for sibling in group:
            socket = ctx.socket(zmq.REQ)
            socket.connect(f"tcp://{sibling}:{DEFAULT_BULLY_PORT}")
            logger.info("%s sending VICTORY to %s", container, sibling)
            socket.send_json({"type": VICTORY, "host": container})
            response = socket.recv_json()
            logger.info("%s got PING from %s", container, sibling)
        logger.info("%s is the leader", container)
        os.environ["LAZARUS_GROUP_LEADER"] = container
        return


class LeaderElectionListener(Thread):
    def __init__(self, port: int = cfg.lazarus.ping_port(default=DEFAULT_BULLY_PORT)):
        super().__init__()
        self.port = port

        ctx = zmq.Context.instance()

        self.socket = ctx.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{self.port}")

    def reply_to_leader_election(self):
        response = self.socket.recv_json()
        if response["type"] == ELECTION:
            logger.info("Received ELECTION")
            self.socket.send_json({"type": PING, "host": cfg.lazarus.identifier()})
            elect_leader(cfg.lazarus.identifier(), cfg.lazarus.groupids().split())
        elif response["type"] == VICTORY:
            logger.info("Received VICTORY")
            self.socket.send_json({"type": PING, "host": cfg.lazarus.identifier()})
            os.environ["LAZARUS_GROUP_LEADER"] = response["host"]

    def run(self):
        while True:
            logger.info("Listening for leader election")
            self.reply_to_leader_election()
