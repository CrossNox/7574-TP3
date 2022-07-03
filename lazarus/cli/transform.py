from typing import List

import typer

from lazarus.mom.queue import Queue
from lazarus.nodes.node import Node
from lazarus.utils import get_logger
from lazarus.sidecar import HeartbeatSender
from lazarus.tasks.transforms import FilterColumn, PostsMeanScore
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange

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
    node_id: int = typer.Argument(..., help="The node id"),
    group_id: int = typer.Argument(..., help="The id of the consumer group"),
    input_queue: str = typer.Argument(..., help="Name of the posts queue"),
    output_exchange: str = typer.Option("posts_mean_score", help="The output exchange"),
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
            output_exchange,
            consumers=[
                ConsumerConfig(
                    f"{output_exchange}-group_{idx}-id_{j}", ConsumerType.Subscriber
                )
                for idx, i in enumerate(subscribers)
                for j in range(i)
            ],
        )
    ]

    node = Node(
        callback=PostsMeanScore,
        queue_in=queue_in,
        exchanges_out=exchanges_out,
        producers=producers,
    )
    node.start()


@app.command()
def filter_columns(
    node_id: int = typer.Argument(..., help="The node id"),
    group_id: int = typer.Argument(..., help="The id of the consumer group"),
    columns: List[str] = typer.Argument(..., help="The list of columns to keep"),
    input_queue: str = typer.Argument(..., help="Name of the input queue"),
    output_exchange: str = typer.Option("filter_columns", help="The output exchange"),
    producers: int = typer.Option(1, help="How many producers to listen from"),
    subscribers: List[int] = typer.Option(
        [1], help="Amount of subscribers in each group of subscribers"
    ),
    rabbit_host: str = typer.Option("rabbitmq", help="The address for rabbitmq"),
):
    heartbeat_sender = HeartbeatSender()
    heartbeat_sender.start()

    logger.info("Reading from %s", f"{input_queue}-group_{group_id}-id_{node_id}")
    queue_in = Queue(rabbit_host, f"{input_queue}-group_{group_id}-id_{node_id}")
    exchanges_out = [
        WorkerExchange(
            rabbit_host,
            output_exchange,
            consumers=[
                ConsumerConfig(
                    f"{output_exchange}-group_{idx}-id_{j}", ConsumerType.Subscriber
                )
                for idx, i in enumerate(subscribers)
                for j in range(i)
            ],
        )
    ]

    node = Node(
        callback=FilterColumn,
        columns=columns,
        queue_in=queue_in,
        exchanges_out=exchanges_out,
        producers=producers,
    )
    node.start()
