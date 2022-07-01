import time

from lazarus.sidecar import HeartbeatsListener
from lazarus.utils import get_logger, config_logging

logger = get_logger(__name__)


def callback(host, port):
    logger.info(f"host {host}:{port} is gone")
    time.sleep(1)


if __name__ == "__main__":
    config_logging(verbose=2, pretty=True)
    hbl = HeartbeatsListener([("localhost", 5555)], callback)
    hbl.run()
