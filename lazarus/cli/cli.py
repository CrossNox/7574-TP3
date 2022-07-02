from typing import List

import typer

from lazarus.sidecar import HeartbeatsListener
from lazarus.cli.filter import app as filter_app
from lazarus.cli.dataset import app as dataset_app
from lazarus.constants import DEFAULT_HEARTBEAT_PORT
from lazarus.cli.transform import app as transform_app
from lazarus.docker_utils import SystemContainer, list_containers_from_config
from lazarus.utils import DEFAULT_PRETTY, DEFAULT_VERBOSE, get_logger, config_logging

logger = get_logger(__name__)

app = typer.Typer()
app.add_typer(dataset_app, name="dataset")
app.add_typer(filter_app, name="filter")
app.add_typer(transform_app, name="transform")


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


class HeartbeatReviverCallback:
    def __init__(self, containers: List[SystemContainer]):
        self.containers = {c.identifier: c.command for c in containers}
        self.raw_containers = containers

    def __call__(self, host, port):
        self.containers[host].revive()


@app.command()
def coordinator():
    containers = list_containers_from_config()
    callback = HeartbeatReviverCallback(containers)

    hbl = HeartbeatsListener(
        [(container.identifier, DEFAULT_HEARTBEAT_PORT) for container in containers],
        callback,
    )
    # hbl = HeartbeatsListener([("localhost", 5555)], callback)

    hbl.start()
    hbl.join()


if __name__ == "__main__":
    app()
