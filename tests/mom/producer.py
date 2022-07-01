#!/usr/bin/env python3
import os
import time

import typer

from lazarus.utils import get_logger
from tests.mom.common import new_msg
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange

logger = get_logger(__name__)

app = typer.Typer()


def producer():
    app_id = int(os.environ["ID"])
    name = f"producer-{app_id}"
    logger.info(f"{name} starting...")
    exchange = WorkerExchange(
        "rabbitmq",
        "producer-exchage",
        [
            ConsumerConfig("worker-0", ConsumerType.Worker),
            ConsumerConfig("worker-1", ConsumerType.Worker),
            ConsumerConfig("worker-2", ConsumerType.Worker),
            ConsumerConfig("subscriber", ConsumerType.Subscriber),
        ],
    )

    for i in range(100):
        msg = new_msg(name, "data", f"Message {i}")
        exchange.push(msg)
        logger.info(f"Sent message {i}")
        time.sleep(0.1)

    msg = new_msg(name, "finish", "")
    exchange.broadcast(msg)
    exchange.close()
    logger.info(f"Producer {app_id} finished, bye!")
