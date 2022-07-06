from typing import List

import typer

from lazarus.cfg import cfg
from lazarus.mom.queue import Queue
from lazarus.nodes.node import Node
from lazarus.sidecar import HeartbeatSender
from lazarus.constants import DEFAULT_DATA_DIR
from lazarus.storage.local import LocalStorage
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.tasks.transforms import (
    FilterColumn,
    CommentFilter,
    PostsMeanScore,
    PostsMeanSentiment,
)
from lazarus.utils import (
    get_logger,
    ensure_path,
    parse_group,
    build_node_id,
    exchange_name,
    queue_in_name,
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
    node_id: int = typer.Argument(..., help="The node id"),
    group_id: str = typer.Option(
        "posts_mean_score", help="The id of the consumer group"
    ),
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

    node_identifier: str = build_node_id(group_id, node_id)

    storage = LocalStorage.load(
        cfg.lazarus.data_dir(cast=ensure_path, default=DEFAULT_DATA_DIR)
        / node_identifier
    )

    node = Node(
        identifier=node_identifier,
        callback=PostsMeanScore,
        queue_in=queue_in,
        exchanges_out=exchanges_out,
        storage=storage,
        producers=input_group_size,
    )
    node.start()


@app.command()
def filter_columns(
    node_id: int = typer.Argument(..., help="The node id"),
    columns: List[str] = typer.Argument(..., help="List of columns to keep"),
    group_id: str = typer.Option(
        "posts_score_above_mean", help="The id of the consumer group"
    ),
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

    node_identifier: str = build_node_id(group_id, node_id)

    storage = LocalStorage.load(
        cfg.lazarus.data_dir(cast=ensure_path, default=DEFAULT_DATA_DIR)
        / node_identifier
    )

    node = Node(
        identifier=node_identifier,
        callback=FilterColumn,
        columns=columns,
        queue_in=queue_in,
        storage=storage,
        exchanges_out=exchanges_out,
        producers=input_group_size,
    )
    node.start()


@app.command()
def filter_comments(
    node_id: int = typer.Argument(..., help="The node id"),
    group_id: str = typer.Option(
        "comments_filter", help="The id of the consumer group"
    ),
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

    node_identifier: str = build_node_id(group_id, node_id)

    storage = LocalStorage.load(
        cfg.lazarus.data_dir(cast=ensure_path, default=DEFAULT_DATA_DIR)
        / node_identifier
    )

    node = Node(
        identifier=node_identifier,
        callback=CommentFilter,
        queue_in=queue_in,
        exchanges_out=exchanges_out,
        storage=storage,
        producers=input_group_size,
    )
    node.start()


@app.command()
def posts_mean_sentiment(
    node_id: int = typer.Argument(..., help="The node id"),
    group_id: str = typer.Option(
        "posts_mean_sentiment", help="The id of the consumer group"
    ),
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

    node_identifier: str = build_node_id(group_id, node_id)

    storage = LocalStorage.load(
        cfg.lazarus.data_dir(cast=ensure_path, default=DEFAULT_DATA_DIR)
        / node_identifier
    )

    node = Node(
        identifier=node_identifier,
        callback=PostsMeanSentiment,
        queue_in=queue_in,
        exchanges_out=exchanges_out,
        storage=storage,
        producers=input_group_size,
    )
    node.start()
