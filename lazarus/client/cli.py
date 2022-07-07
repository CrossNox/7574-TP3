from pathlib import Path
from typing import List

import typer

from lazarus.cfg import cfg
from lazarus.client.client import Client
from lazarus.constants import DEFAULT_MEME_PATH
from lazarus.utils import DEFAULT_PRETTY, DEFAULT_VERBOSE, get_logger, config_logging

logger = get_logger(__name__)

app = typer.Typer()


@app.command()
def main(
    posts: Path = typer.Argument(..., help="Path to posts csv file"),
    comments: Path = typer.Argument(..., help="Path to comments csv file"),
    hosts: List[str] = typer.Argument(..., help="List of server hosts to connect from"),
    download_dir: Path = typer.Option(
        cfg.lazarus.download_dir(default=DEFAULT_MEME_PATH),
        help="Path to download meme to",
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
    """Client entrypoint."""
    config_logging(verbose, pretty)

    client = Client(hosts, posts, comments, download_dir)

    client.run()
