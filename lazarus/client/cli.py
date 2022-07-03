import csv
import multiprocessing as mp
from pathlib import Path
from typing import List

import typer

from lazarus.constants import EOS
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.mom.message import Message
from lazarus.utils import (
    DEFAULT_PRETTY,
    DEFAULT_VERBOSE,
    get_logger,
    parse_group,
    exchange_name,
    queue_in_name,
    config_logging,
)

logger = get_logger(__name__)

app = typer.Typer()


def relay_file(
    rabbit_host: str, exchange: str, file_path: Path, queue: str, groups: List[str]
):
    # TODO: get session id

    groups = [parse_group(group) for group in groups]

    exchanges = [
        WorkerExchange(
            rabbit_host,
            exchange_name(queue, group_id),
            [
                ConsumerConfig(
                    queue_in_name(queue, group_id, node_id), ConsumerType.Worker
                )
                for node_id in range(group_size)
            ],
        )
        for group_id, group_size in groups
    ]

    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)
        for line in reader:
            m = {"type": "data", "session_id": 1, "data": line}  # TODO: Hardcoded
            msg = Message(data=m)
            for exch in exchanges:
                exch.push(msg)

        m = {"type": EOS, "session_id": 1}  # TODO: Hardcoded
        for exch in exchanges:
            exch.broadcast(Message(data=m))

    exch.close()


@app.command()
def main(
    posts: Path = typer.Argument(..., help="Path to posts csv file"),
    comments: Path = typer.Argument(..., help="Path to comments csv file"),
    rabbit_host: str = typer.Argument(..., help="RabbitMQ address"),
    posts_exchange: str = typer.Option("posts", help="Name of the posts exchange"),
    comments_exchange: str = typer.Option(
        "comments", help="Name of the comments exchange"
    ),
    comments_groups: List[str] = typer.Option(..., help="<group_id>:<n>"),
    posts_groups: List[str] = typer.Option(..., help="<group_id>:<n>"),
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
    """Client entrypoint."""
    config_logging(verbose, pretty)
    logger.info("Starting processes")

    pposts = mp.Process(
        target=relay_file,
        args=(rabbit_host, posts_exchange, posts, "posts", posts_groups),
    )
    pcomments = mp.Process(
        target=relay_file,
        args=(rabbit_host, comments_exchange, comments, "comments", comments_groups),
    )

    logger.info("Starting posts relay process")
    pposts.start()

    logger.info("Starting comments relay process")
    pcomments.start()

    pposts.join()
    logger.info("Joined posts relay process")

    pcomments.join()
    logger.info("Joined comments relay process")
