import os
from typing import List
from lazarus.server.server import Server
from lazarus.client.client import Client
from lazarus.utils import config_logging


def run_client(hosts: List[str], port: str):
    client = Client(hosts, port)
    client.run()

def run_server(port: str, is_leader: bool):
    server = Server(port, is_leader)
    server.run()

def main():
    config_logging(1, True)
    ptype = os.environ["APP_TYPE"]
    port = '8000'
    n_servers = 2
    hosts = []

    for s_id in range(n_servers, 0, -1):
        host = f"server{s_id}"
        hosts.append(host)

    if ptype == "client":
        run_client(hosts, port)
    elif ptype == "server":
        is_leader = int(os.environ["IS_LEADER"]) > 0
        run_server(port, is_leader)
    else:
        print("Couldn't parse app type")


if __name__ == "__main__":
    main()
