import csv
from pathlib import Path
import multiprocessing as mp

import typer

from lazarus.constants import EOS
from lazarus.mom.message import Message
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.utils import DEFAULT_PRETTY, DEFAULT_VERBOSE, get_logger, config_logging

logger = get_logger(__name__)

app = typer.Typer()


def relay_file(
    rabbit_host: str, exchange: str, file_path: Path, queue: str, _nworkers: int
):

    exch = WorkerExchange(
        rabbit_host, exchange, [ConsumerConfig(queue, ConsumerType.Worker)]
    )

    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)
        for line in reader:
            msg = Message(data=line)
            exch.push(msg)

        exch.broadcast(Message(data=EOS))

    exch.close()


@app.command()
def main(
    posts: Path = typer.Argument(..., help="Path to posts csv file"),
    comments: Path = typer.Argument(..., help="Path to comments csv file"),
    rabbit_host: str = typer.Argument(..., help="RabbitMQ address"),
    posts_queue: str = typer.Option("posts", help="Name of the posts queue"),
    comments_queue: str = typer.Option("comments", help="Name of the comments queue"),
    nworkers: int = typer.Option(1, help="The amount of downstream workers"),
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

    posts_exchange = "posts-exchange"
    comments_exchange = "comments-exchange"

    pposts = mp.Process(
        target=relay_file,
        args=(rabbit_host, posts_exchange, posts, posts_queue, nworkers),
    )
    pcomments = mp.Process(
        target=relay_file,
        args=(rabbit_host, comments_exchange, comments, comments_queue, nworkers),
    )

    logger.info("Starting posts relay process")
    pposts.start()

    logger.info("Starting comments relay process")
    pcomments.start()

    pposts.join()
    logger.info("Joined posts relay process")

    pcomments.join()
    logger.info("Joined comments relay process")
