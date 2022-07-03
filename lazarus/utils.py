"""Utils file."""

import logging
import pathlib
from typing import Callable

import typer

import pika

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


def get_logger(name: str):
    return logging.getLogger(name)


utils_logger = get_logger(__name__)


def path_or_none(s):
    if s == "":
        return None
    return pathlib.Path(s)


def coalesce(f: Callable) -> Callable:
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
            utils_logger.error(f"error calling {f.__name__}", exc_info=True)
            return None

    return _inner
