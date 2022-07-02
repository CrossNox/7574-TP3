from typing import List

import typer

from lazarus.mom.queue import Queue
from lazarus.nodes.node import Node
from lazarus.utils import get_logger
from lazarus.sidecar import HeartbeatSender
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.tasks.transforms import (
    FilterColumn,
    ExtractPostID,
    PostsMeanScore,
    PostsMeanSentiment,
)

logger = get_logger(__name__)

app = typer.Typer()


# @app.command()
# def extract_post_id():
#     heartbeat_sender = HeartbeatSender()
#     heartbeat_sender.start()

#     node = Node(callback=ExtractPostID)
#     node.start()


# @app.command()
# def posts_mean_sentiment():
#     heartbeat_sender = HeartbeatSender()
#     heartbeat_sender.start()

#     node = Node(callback=PostsMeanSentiment)
#     node.start()


@app.command()
def posts_mean_score(
    rabbit_host: str = typer.Option("rabbitmq", help="The address for rabbitmq"),
    input_queue: str = typer.Option("post-averager", help="Name of the posts queue"),
):
    heartbeat_sender = HeartbeatSender()
    heartbeat_sender.start()

    queue_in = Queue(rabbit_host, input_queue)
    exchange_name = (
        "post-averager-exchange"  # This can be hardcoded, maybe constants.py
    )
    exchanges_out = [
        WorkerExchange(
            rabbit_host,
            exchange_name,
            consumers=[
                ConsumerConfig(
                    "post-averager-result", ConsumerType.Subscriber
                )  # TODO: Check
            ],
        )
    ]

    node = Node(callback=PostsMeanScore, queue_in=queue_in, exchanges_out=exchanges_out)
    node.start()


@app.command()
def filter_columns(
    columns: List[str] = typer.Argument(..., help="The list of columns to keep"),
    rabbit_host: str = typer.Option("rabbitmq", help="The address for rabbitmq"),
    posts_queue: str = typer.Option("posts", help="Name of the posts queue"),
):
    heartbeat_sender = HeartbeatSender()
    heartbeat_sender.start()

    queue_in = Queue(rabbit_host, posts_queue)
    exchange_name = (
        "post-transform-exchange"  # This can be hardcoded, maybe constants.py
    )
    exchanges_out = [
        WorkerExchange(
            rabbit_host,
            exchange_name,
            consumers=[
                ConsumerConfig("post-averager", ConsumerType.Subscriber)  # TODO: Check
            ],
        )
    ]

    node = Node(
        callback=FilterColumn,
        columns=columns,
        queue_in=queue_in,
        exchanges_out=exchanges_out,
    )
    node.start()
