import os

from tests.mom.worker import worker
from tests.mom.producer import producer
from lazarus.utils import config_logging
from tests.mom.collector import collector
from tests.mom.subscriber import subscriber


def main():

    config_logging(1, True)
    ptype = os.environ["APP_TYPE"]
    if ptype == "producer":
        producer()
    elif ptype == "subscriber":
        subscriber()
    elif ptype == "worker":
        worker()
    elif ptype == "collector":
        collector()
    else:
        print("Couldn't parse app type")


if __name__ == "__main__":
    main()
