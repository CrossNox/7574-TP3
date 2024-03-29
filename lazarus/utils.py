"""Utils file."""

import base64
import logging
import pathlib
from typing import Tuple, Callable

import pika
import typer
import urllib3

import docker

DEFAULT_PRETTY = False

DEFAULT_VERBOSE = 0


class TyperLoggerHandler(logging.Handler):
    def __init__(self, pretty: bool, *args, **kwargs):
        self.pretty = pretty
        super().__init__(*args, **kwargs)

    def emit(self, record: logging.LogRecord) -> None:
        if not self.pretty:
            typer.secho(self.format(record))
            return

        fg = None
        bg = None
        if record.levelno == logging.DEBUG:
            fg = typer.colors.BLACK
            bg = typer.colors.WHITE
        elif record.levelno == logging.INFO:
            fg = typer.colors.BRIGHT_BLUE
        elif record.levelno == logging.WARNING:
            fg = typer.colors.BRIGHT_MAGENTA
        elif record.levelno == logging.CRITICAL:
            fg = typer.colors.BRIGHT_RED
        elif record.levelno == logging.ERROR:
            fg = typer.colors.BLACK
            bg = typer.colors.BRIGHT_RED
        typer.secho(self.format(record), bg=bg, fg=fg)


def config_logging(verbose: int = DEFAULT_VERBOSE, pretty: bool = DEFAULT_PRETTY):
    """Configure logging for stream and file."""

    level = logging.ERROR
    if verbose == 1:
        level = logging.INFO
    elif verbose > 1:
        level = logging.DEBUG

    logger = logging.getLogger()

    logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    typer_handler = TyperLoggerHandler(pretty=pretty)
    typer_handler.setLevel(level)
    typer_handler.setFormatter(formatter)
    logger.addHandler(typer_handler)

    pika_logger = logging.getLogger(pika.__name__)
    pika_logger.setLevel(logging.ERROR)

    docker_logger = logging.getLogger(docker.__name__)
    docker_logger.setLevel(logging.ERROR)

    urllib3_logger = logging.getLogger(urllib3.__name__)
    urllib3_logger.setLevel(logging.ERROR)


def get_logger(name: str):
    return logging.getLogger(name)


utils_logger = get_logger(__name__)


def path_or_none(s):
    if s == "":
        return None
    return pathlib.Path(s)


def ensure_path(path):
    """Create a path if it does not exist."""
    path = pathlib.Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_file_directory(path):
    """Create a parent directories for a file's path if they do not exist."""
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def coalesce(f: Callable, log: bool = True) -> Callable:
    """Wrap a function to return None on raised exceptions.

    This function makes functions that might raise exception safe for `map`.
    If an exception is raised, None is returned instead.

    Parameters
    ----------
        f: Callable to wrap

    Returns
    -------
        Wrapped callable

    """

    def _inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:  # pylint: disable=bare-except  # noqa: E722
            if log:
                utils_logger.error(f"error calling {f.__name__}", exc_info=True)
            return None

    return _inner


def binary_to_ascii(data: bytes) -> str:
    b64 = base64.b64encode(data)
    return b64.decode("ascii")


def ascii_to_binary(data: str) -> bytes:
    b64 = data.encode("ascii")
    return base64.b64decode(b64)


def exchange_name(group_id: str, output_group_id: str) -> str:
    return f"{group_id}::{output_group_id}"


def parse_group(group: str) -> Tuple[str, int]:
    group, size = group.split(":")
    return group, int(size)


def build_node_id(group_id: str, node_id: int) -> str:
    return f"{group_id}_{node_id}"


def queue_in_name(input_group_id: str, group_id: str, node_id: int) -> str:
    return f"{input_group_id}::{build_node_id(group_id, node_id)}"
