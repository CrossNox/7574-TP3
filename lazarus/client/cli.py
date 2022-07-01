import csv
import json
from pathlib import Path
import multiprocessing as mp

import pika
import typer

from lazarus.utils import DEFAULT_PRETTY, DEFAULT_VERBOSE, get_logger, config_logging

logger = get_logger(__name__)

app = typer.Typer()


def relay_file(rabbit_host: str, file_path: Path, exchange: str):
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host))
    channel = conn.channel()
    channel.exchange_declare(exchange="client", exchange_type="direct")

    channel.confirm_delivery()

    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)
        for line in reader:
            try:
                channel.basic_publish(
                    exchange="client",
                    routing_key=exchange,
                    body=json.dumps(line),
                    properties=pika.BasicProperties(
                        content_type="text/json",
                        delivery_mode=pika.DeliveryMode.Transient,
                    ),
                )
            except pika.exceptions.UnroutableError:
                logger.error("Message could not be confirmed")
        try:
            channel.basic_publish(
                exchange="client",
                routing_key=exchange,
                body="",
                properties=pika.BasicProperties(
                    content_type="text/json",
                    delivery_mode=pika.DeliveryMode.Transient,
                ),
            )
        except pika.exceptions.UnroutableError:
            logger.error("Message could not be confirmed")


@app.command()
def main(
    posts: Path = typer.Argument(..., help="Path to posts csv file"),
    comments: Path = typer.Argument(..., help="Path to comments csv file"),
    rabbit_host: str = typer.Argument(..., help="RabbitMQ address"),
    posts_exchange: str = typer.Option("posts", help="Name of the posts exchange"),
    comments_exchange: str = typer.Option(
        "comments", help="Name of the comments exchange"
    ),
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
    logger.info("Starting processes")

    pposts = mp.Process(target=relay_file, args=(rabbit_host, posts, posts_exchange))
    pcomments = mp.Process(
        target=relay_file, args=(rabbit_host, comments, comments_exchange)
    )

    logger.info("Starting posts relay process")
    pposts.start()

    logger.info("Starting comments relay process")
    pcomments.start()

    logger.info("Joining posts relay process")
    pposts.join()

    logger.info("Joining comments relay process")
    pcomments.join()
