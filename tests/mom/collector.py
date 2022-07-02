#!/usr/bin/env python3
import threading as th

from lazarus.mom.queue import Queue
from lazarus.utils import get_logger
from lazarus.mom.message import Message
from tests.mom.common import all_finished

logger = get_logger(__name__)


def collector():
    name = "collector"
    queue = Queue("rabbitmq", name)
    logger.info(f"{name} starting...")

    should_finish = th.Event()

    finish_table = {
        "worker-0": False,
        "worker-1": False,
        "worker-2": False,
        "subscriber": False,
    }

    results = {}

    def callback(msg: Message):
        if msg["type"] == "finish":
            logger.info(f"[{id}] Received finish message from {msg['from']}")
            finish_table[msg["from"]] = True
            msg.ack()
            if all_finished(finish_table):
                should_finish.set()
            return
        results[msg["from"]] = msg["data"]
        msg.ack()

    queue.consume(callback)
    should_finish.wait()
    queue.close()

    logger.info("Computation results: ")
    logger.info(f"- Worker 0: {results['worker-0']}")
    logger.info(f"- Worker 1: {results['worker-1']}")
    logger.info(f"- Worker 2: {results['worker-2']}")
    logger.info(f"- Subscriber: {results['subscriber']}")

    logger.info(f"Worker-{id} finished, bye!")
