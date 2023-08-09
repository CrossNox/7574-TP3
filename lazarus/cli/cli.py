from typing import List
from multiprocessing import Array, Event, Process

import typer

from lazarus.cfg import cfg
from lazarus.mom.queue import Queue
from lazarus.nodes.node import Node
from lazarus.tasks.joiner import Joiner
from lazarus.server.server import Server
from lazarus.tasks.collect import Collector
from lazarus.cli.sink import app as sink_app
from lazarus.storage.local import LocalStorage
from lazarus.cli.filter import app as filter_app
from lazarus.cli.dataset import app as dataset_app
from lazarus.cli.download import app as download_app
from lazarus.cli.transform import app as transform_app
from lazarus.bully import LeaderElectionListener, elect_leader
from lazarus.sidecar import HeartbeatSender, HeartbeatsListener
from lazarus.docker_utils import SystemContainer, list_containers_from_config
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.constants import (
    UNKNOWN,
    LOOKINGFOR,
    BULLY_TIMEOUT_MS,
    DEFAULT_DATA_DIR,
    MAX_IDENTIFIER_SIZE,
    DEFAULT_HEARTBEAT_PORT,
)
from lazarus.utils import (
    DEFAULT_PRETTY,
    DEFAULT_VERBOSE,
    get_logger,
    ensure_path,
    parse_group,
    build_node_id,
    exchange_name,
    queue_in_name,
    config_logging,
)

logger = get_logger(__name__)

app = typer.Typer()
app.add_typer(dataset_app, name="dataset")
app.add_typer(filter_app, name="filter")
app.add_typer(transform_app, name="transform")
app.add_typer(download_app, name="download")
app.add_typer(sink_app, name="sink")


@app.callback()
def main(
    verbose: int = typer.Option(
        DEFAULT_VERBOSE,
        "--verbose",
        "-v",
        count=True,
        help="Level of verbosity. Can be passed more than once for more levels of logging.",
    ),
    pretty: bool = typer.Option(
        DEFAULT_PRETTY, "--pretty", help="Whether to pretty print the logs with colors"
    ),
):
    config_logging(verbose, pretty)


class ElectLeader:
    def __init__(self, node_id: str, group: List[str], leader_value):
        self.node_id = node_id
        self.group = group
        self.leader_value = leader_value

    def __call__(self, host, port):
        if self.leader_value.value in (host, UNKNOWN, LOOKINGFOR):
            # elect_leader(self.node_id, self.group, self.leader_value)
            p = Process(
                target=elect_leader, args=(self.node_id, self.group, self.leader_value)
            )
            p.start()
            p.join()


@app.command()
def server(
    server_id: int = typer.Argument(...),
    group_identifier: str = typer.Option("server"),
    group_size: int = typer.Argument(...),
    posts_group: List[str] = typer.Option(...),
    comments_group: List[str] = typer.Option(...),
    results_queue: str = typer.Argument(...),
):

    node_id = build_node_id(group_identifier, server_id)
    group = [build_node_id(group_identifier, i) for i in range(group_size)]

    hosts = [(h, DEFAULT_HEARTBEAT_PORT) for h in filter(lambda x: x != node_id, group)]
    hbs = HeartbeatSender(node_id)

    leader_value = Array("c", MAX_IDENTIFIER_SIZE)
    leader_value.value = UNKNOWN.encode()
    logger.info("Initial leader_value.value is %s", leader_value.value)

    lel = LeaderElectionListener(node_id, group, leader_value)
    lel.start()

    all_healthy = Event()

    hbl = HeartbeatsListener(
        hosts,
        ElectLeader(node_id, group, leader_value),
        sleep_time=BULLY_TIMEOUT_MS // 1000,
        # first_healthy_callback=HealthyElectionCallback(node_id, group, leader_value,),
        all_healthy=all_healthy,
    )

    hbs.start()
    hbl.start()

    logger.info("Waiting for all healthy lock")
    all_healthy.wait()
    logger.info("Waiting for all healthy lock")
    elect_leader(node_id, group, leader_value)
    logger.info("Leader election over, %s", leader_value.value.decode())

    new_server = Server(
        server_id,
        group_identifier,
        group_size,
        posts_group,
        comments_group,
        results_queue,
        leader_value,
    )
    new_server.start()


class HeartbeatReviverCallback:
    def __init__(self, containers: List[SystemContainer]):
        self.containers = {c.identifier: c for c in containers}

    def __call__(self, host, port):
        container = self.containers[host]
        container.revive()


@app.command()
def coordinator():
    containers = list_containers_from_config()
    # for container in containers:
    #    container.revive()

    callback = HeartbeatReviverCallback(containers)
    hbl = HeartbeatsListener(
        [(container.identifier, DEFAULT_HEARTBEAT_PORT) for container in containers],
        callback,
    )

    hbl.start()
    hbl.join()


@app.command()
def collect(
    node_id: int = typer.Argument(..., help="The node id"),
    keep: List[str] = typer.Argument(..., help="Columns to keep from each input"),
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
    node_identifier: str = build_node_id(group_id, node_id)

    heartbeat_sender = HeartbeatSender(node_identifier)
    heartbeat_sender.start()

    queues_in: List[Queue] = []
    n_eos: List[int] = []

    for group in input_group:
        group_in_id, group_in_size = parse_group(group)
        queues_in.append(
            Queue(rabbit_host, queue_in_name(group_in_id, group_id, node_id))
        )
        n_eos.append(group_in_size)

    parsed_output_groups = [
        parse_group(group)
        if group != "servers"
        else ("servers", cfg.lazarus.servers(cast=int))
        for group in output_groups
    ]

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
        if output_group_id != "servers"
        else WorkerExchange(
            rabbit_host,
            exchange_name(group_id, output_group_id),
            consumers=[
                ConsumerConfig(f"{group_id}::servers", ConsumerType.Subscriber)
                for j in range(output_group_size)
            ],
        )
        for output_group_id, output_group_size in parsed_output_groups
    ]

    storage = LocalStorage.load(
        cfg.lazarus.data_dir(cast=ensure_path, default=DEFAULT_DATA_DIR)
        / node_identifier
    )

    node = Node(
        identifier=node_identifier,
        callback=Collector,
        keep=dict(zip([q.queue_name for q in queues_in], keep)),
        queue_in=queues_in,
        exchanges_out=exchanges_out,
        storage=storage,
        producers=n_eos,
    )
    node.start()


@app.command()
def join(
    node_id: int = typer.Argument(..., help="The node id"),
    merge_keys: List[str] = typer.Argument(..., help="The keys to merge on the tables"),
    group_id: str = typer.Option(
        ...,
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
    node_identifier: str = build_node_id(group_id, node_id)

    heartbeat_sender = HeartbeatSender(node_identifier)
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

    storage = LocalStorage.load(
        cfg.lazarus.data_dir(cast=ensure_path, default=DEFAULT_DATA_DIR)
        / node_identifier
    )

    merge_keys_kwargs = dict(zip([q.queue_name for q in queues_in], merge_keys))

    node = Node(
        identifier=node_identifier,
        callback=Joiner,
        queue_in=queues_in,
        exchanges_out=exchanges_out,
        storage=storage,
        producers=n_eos,
        dependencies=None,
        **merge_keys_kwargs,
    )

    node.start()


if __name__ == "__main__":
    app()
