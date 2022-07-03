import csv
import multiprocessing as mp
from pathlib import Path
from typing import List

import typer

from lazarus.constants import EOS
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.mom.message import Message
from lazarus.utils import DEFAULT_PRETTY, DEFAULT_VERBOSE, get_logger, config_logging

logger = get_logger(__name__)

app = typer.Typer()


def relay_file(
    rabbit_host: str, exchange: str, file_path: Path, queue: str, consumers: List[int]
):
    # TODO: get session id
    exch = WorkerExchange(
        rabbit_host,
        exchange,
        [
            ConsumerConfig(f"{queue}-group_{idx}-id_{j}", ConsumerType.Worker)
            for idx, i in enumerate(consumers)
            for j in range(i)
        ],
    )

    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)
        for line in reader:
            m = {"type": "data", "session_id": 1, "data": line}  # TODO: Hardcoded
            msg = Message(data=m)
            exch.push(msg)

        m = {"type": EOS, "session_id": 1}  # TODO: Hardcoded
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
    consumers: List[int] = typer.Option([1], help="Amount of consumers in each group"),
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
        args=(rabbit_host, posts_exchange, posts, "posts", consumers),
    )
    pcomments = mp.Process(
        target=relay_file,
        args=(rabbit_host, comments_exchange, comments, "comments", consumers),
    )

    logger.info("Starting posts relay process")
    pposts.start()

    logger.info("Starting comments relay process")
    pcomments.start()

    pposts.join()
    logger.info("Joined posts relay process")

    pcomments.join()
    logger.info("Joined comments relay process")
