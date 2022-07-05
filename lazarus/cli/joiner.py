from typing import List

import typer

from lazarus.mom.queue import Queue
from lazarus.nodes.node import Node
from lazarus.tasks.joiner import Joiner
from lazarus.sidecar import HeartbeatSender
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.utils import get_logger, parse_group, exchange_name, queue_in_name

logger = get_logger(__name__)

app = typer.Typer()


@app.command()
def joiner(
    node_id: int = typer.Argument(..., help="The node id"),
    merge_key: str = typer.Argument("id", help="The key to merge on the tables"),
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

    queues_in: List[Queue] = []
    n_eos: List[int] = []

    for group in input_group:
        group_in_id, group_in_size = parse_group(group)
        queues_in.append(
            Queue(rabbit_host, queue_in_name(group_in_id, group_id, node_id))
        )
        n_eos.append(group_in_size)

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

    node = Node(
        callback=Joiner,
        merge_key=merge_key,
        queue_in=queues_in,
        exchanges_out=exchanges_out,
        producers=n_eos,
    )

    node.start()
