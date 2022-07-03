from typing import List

import typer

from lazarus.mom.queue import Queue
from lazarus.nodes.node import Node
from lazarus.utils import get_logger
from lazarus.sidecar import HeartbeatSender
from lazarus.tasks.filters import FilterPostsScoreAboveMean
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange

logger = get_logger(__name__)

app = typer.Typer()


# @app.command()
# def uniq_posts():
#     heartbeat_sender = HeartbeatSender()
#     heartbeat_sender.start()
#
#     node = Node(callback=FilterUniqIDs)
#     node.start()


@app.command()
def posts_score_above_mean(
    node_id: int = typer.Argument(..., help="The node id"),
    group_id: int = typer.Argument(..., help="The id of the consumer group"),
    mean_queue: str = typer.Argument(..., help="Queue where to fetch mean from"),
    input_queue: str = typer.Argument(..., help="Name of the input queue"),
    output_exchange: str = typer.Option(
        "posts_score_above_mean", help="The output exchange"
    ),
    producers: int = typer.Option(1, help="How many producers to listen from"),
    subscribers: List[int] = typer.Option(
        [1], help="Amount of subscribers in each group of subscribers"
    ),
    rabbit_host: str = typer.Option("rabbitmq", help="The address for rabbitmq"),
):
    heartbeat_sender = HeartbeatSender()
    heartbeat_sender.start()

    queue_in = Queue(rabbit_host, f"{input_queue}-group_{group_id}-id_{node_id}")
    exchanges_out = [
        WorkerExchange(
            rabbit_host,
            f"{output_exchange}_{idx}",
            consumers=[
                ConsumerConfig(
                    f"{output_exchange}-group_{idx}-id_{j}",
                    ConsumerType.Worker if i > 1 else ConsumerType.Subscriber,
                )
                for j in range(i)
            ],
        )
        for idx, i in enumerate(subscribers)
    ]

    node = Node(
        callback=FilterPostsScoreAboveMean,
        queue_in=queue_in,
        exchanges_out=exchanges_out,
        dependencies={"posts_mean_score": Queue(rabbit_host, mean_queue)},
        producers=producers,
    )
    node.start()


# @app.command()
# def ed_comments():
#     heartbeat_sender = HeartbeatSender()
#     heartbeat_sender.start()
#
#     node = Node(callback=FilterEdComment)
#     node.start()


# @app.command()
# def nan_sentiment():
#    heartbeat_sender = HeartbeatSender()
#    heartbeat_sender.start()
#
#    node = Node(callback=FilterNanSentiment)
#    node.start()


# @app.command()
# def null_url():
#    heartbeat_sender = HeartbeatSender()
#    heartbeat_sender.start()
#
#    node = Node(callback=FilterNullURL)
#    node.start()
