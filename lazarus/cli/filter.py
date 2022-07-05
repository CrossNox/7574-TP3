from typing import List

import typer

from lazarus.cfg import cfg
from lazarus.constants import DEFAULT_DATA_DIR
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.mom.queue import Queue
from lazarus.nodes.node import Node
from lazarus.sidecar import HeartbeatSender
from lazarus.storage.local import LocalStorage
from lazarus.tasks.filters import FilterPostsScoreAboveMean
from lazarus.utils import (
    get_logger,
    ensure_path,
    parse_group,
    exchange_name,
    queue_in_name,
)

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
    group_id: str = typer.Option(
        "posts_score_above_mean", help="The id of the consumer group"
    ),
    mean_queue: str = typer.Argument(..., help="Queue where to fetch mean from"),
    input_group: str = typer.Option(
        ..., help="<name>:<n_producers> of the input group"
    ),
    output_groups: List[str] = typer.Option(
        ..., help="<name>:<n_subscribers> of the output groups"
    ),
    rabbit_host: str = typer.Option("rabbitmq", help="The address for rabbitmq"),
):
    heartbeat_sender = HeartbeatSender()
    heartbeat_sender.start()

    input_group_id, input_group_size = parse_group(input_group)

    parsed_output_groups = [parse_group(group) for group in output_groups]

    queue_in = Queue(rabbit_host, queue_in_name(input_group_id, group_id, node_id))
    exchanges_out = [
        WorkerExchange(
            rabbit_host,
            exchange_name(group_id, output_group_id),
            consumers=[
                ConsumerConfig(
                    queue_in_name(group_id, output_group_id, j),
                    ConsumerType.Worker
                    if output_group_size > 1
                    else ConsumerType.Subscriber,
                )
                for j in range(output_group_size)
            ],
        )
        for output_group_id, output_group_size in parsed_output_groups
    ]

    storage = LocalStorage(
        cfg.lazarus.data_dir(cast=ensure_path, default=DEFAULT_DATA_DIR)
    )

    node = Node(
        callback=FilterPostsScoreAboveMean,
        queue_in=queue_in,
        exchanges_out=exchanges_out,
        storage=storage,
        dependencies={"posts_mean_score": Queue(rabbit_host, mean_queue)},
        producers=input_group_size,
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
