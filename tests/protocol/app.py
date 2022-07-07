import os
from typing import List

from lazarus.client.client import Client
from lazarus.utils import config_logging


def run_client(hosts: List[str]):
    # NOTE: This tests were created only to test the protocol
    # so they do not support data reading and transfer, this is a mock
    client = Client(hosts, "posts.csv", "comments.csv")
    client.run()


def run_server(_is_leader: bool):
    pass
    # server = Server(is_leader)
    # server.run()


def main():
    config_logging(1, True)
    ptype = os.environ["APP_TYPE"]
    n_servers = 2
    hosts = []

    for s_id in range(n_servers, 0, -1):
        host = f"server{s_id}"
        hosts.append(host)

    if ptype == "client":
        run_client(hosts)
    elif ptype == "server":
        is_leader = int(os.environ["IS_LEADER"]) > 0
        run_server(is_leader)
    else:
        print("Couldn't parse app type")


if __name__ == "__main__":
    main()
