import time
import argparse

from lazarus.sidecar import HeartbeatsListener
from lazarus.utils import get_logger, config_logging

logger = get_logger(__name__)


def callback(host, port):
    logger.info(f"host {host}:{port} is gone")
    time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ports", nargs="+", type=int)
    args = parser.parse_args()

    config_logging(verbose=2, pretty=True)
    hbl = HeartbeatsListener([("localhost", port) for port in args.ports], callback)
    hbl.start()
    hbl.join()
