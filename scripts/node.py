from lazarus.sidecar import HeartbeatSender
from lazarus.utils import config_logging

if __name__ == "__main__":
    config_logging(verbose=2, pretty=True)
    hbs = HeartbeatSender(port=5555)
    hbs.run()
