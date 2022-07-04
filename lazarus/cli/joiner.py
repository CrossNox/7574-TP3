from typing import List

import typer

from lazarus.mom.queue import Queue
from lazarus.nodes.node import JoinNode
from lazarus.tasks.joiner import Joiner
from lazarus.sidecar import HeartbeatSender
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.utils import get_logger, parse_group, exchange_name, queue_in_name

logger = get_logger(__name__)

app = typer.Typer()


@app.command()
def joiner(
    node_id: int = typer.Argument(..., help="The node id"),
    columns: List[str] = typer.Argument(..., help="List of columns to keep"),
    group_id: str = typer.Option(
        "sentiment_joiner",
        help="The id of the consumer group",
    ),
    input_group: List[str] = typer.Option(
        ..., help="<name>:<n_subscribers> of the input groups"
    ),
    output_groups: List[str] = typer.Option(
        ..., help="<name>:<n_subscribers> of the output groups"
    ),
    rabbit_host: str = typer.Option("rabbitmq", help="The address for rabbitmq"),
):
    heartbeat_sender = HeartbeatSender()
    heartbeat_sender.start()

    # TODO: We have to refactor this
    i_group_add, i_group_size_add = parse_group(input_group[0])
    i_group_join, i_group_size_join = parse_group(input_group[1])

    add_queue = Queue(rabbit_host, queue_in_name(i_group_add, group_id, node_id))
    join_queue = Queue(rabbit_host, queue_in_name(i_group_join, group_id, node_id))

    parsed_output_groups = [parse_group(group) for group in output_groups]

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

    node = JoinNode(
        Joiner(columns),
        add_queue=add_queue,
        join_queue=join_queue,
        exchanges_out=exchanges_out,
        add_producers=i_group_size_add,
        join_producers=i_group_size_join,
    )

    node.start()
