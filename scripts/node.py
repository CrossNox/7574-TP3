import argparse

from lazarus.utils import config_logging
from lazarus.sidecar import HeartbeatSender

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5555)
    args = parser.parse_args()
    config_logging(verbose=2, pretty=True)
    hbs = HeartbeatSender(port=args.port)
    hbs.start()
    hbs.join()
