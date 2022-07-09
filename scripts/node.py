import argparse

from lazarus.sidecar import HeartbeatSender
from lazarus.utils import config_logging

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5555)
    args = parser.parse_args()
    config_logging(verbose=2, pretty=True)
    hbs = HeartbeatSender("nodito", port=args.port)
    hbs.start()
    hbs.join()
