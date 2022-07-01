#!/usr/bin/env python3
import os
import threading as th

from lazarus.mom.queue import Queue
from lazarus.utils import get_logger
from lazarus.mom.message import Message
from lazarus.mom.exchange import BasicExchange
from tests.mom.common import new_msg, all_finished

logger = get_logger(__name__)


def worker():
    app_id = os.environ["ID"]
    name = f"worker-{app_id}"
    input_queue = Queue("rabbitmq", name)
    output_exchange = BasicExchange("rabbitmq", "result-exchange", "collector")
    logger.info(f"{name} starting...")

    should_finish = th.Event()

    finish_table = {
        "producer-0": False,
        "producer-1": False,
        "producer-2": False,
    }

    state = {"received": 0}

    def callback(msg: Message):
        if msg["type"] == "finish":
            logger.info(f"[{app_id}] Received finish message from {msg['from']}")
            finish_table[msg["from"]] = True
            msg.ack()
            if all_finished(finish_table):
                should_finish.set()
            return
        state["received"] += 1
        logger.info(f"[{app_id}] Received {msg['data']} from {msg['from']}")
        msg.ack()

    input_queue.consume(callback)
    should_finish.wait()
    input_queue.close()

    msg = new_msg(name, "data", f'{state["received"]}')
    finish = new_msg(name, "finish", "")
    output_exchange.push(msg)
    output_exchange.broadcast(finish)
    output_exchange.close()

    logger.info(f"Worker-{app_id} finished, bye!")
