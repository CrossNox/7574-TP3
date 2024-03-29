import csv
import random
from typing import List
from pathlib import Path
from multiprocessing import Process

import typer

from lazarus.constants import EOS
from lazarus.mom.message import Message
from lazarus.mom.exchange import ConsumerType, ConsumerConfig, WorkerExchange
from lazarus.utils import get_logger, parse_group, exchange_name, queue_in_name

logger = get_logger(__name__)

app = typer.Typer()


class FileProvider(Process):
    def __init__(
        self,
        session_id: str,
        rabbit_host: str,
        exchange: str,
        file_path: Path,
        groups: List[str],
        duplicates: float = 0.0,
    ):
        super().__init__()

        self.session_id = session_id
        self.rabbit_host = rabbit_host
        self.file_path = file_path
        self.exchange_name = exchange
        self.groups = groups
        self.duplicates = duplicates

    def run(self):
        try:
            parsed_groups = [parse_group(group) for group in self.groups]

            exchanges = [
                WorkerExchange(
                    self.rabbit_host,
                    exchange_name(self.exchange_name, group_id),
                    [
                        ConsumerConfig(
                            queue_in_name(self.exchange_name, group_id, node_id),
                            ConsumerType.Worker,
                        )
                        for node_id in range(group_size)
                    ],
                )
                for group_id, group_size in parsed_groups
            ]

            with open(self.file_path, newline="") as f:
                reader = csv.DictReader(f)
                logger.info(f"Starting to read {self.file_path}")
                count = 0
                for line in reader:
                    count += 1
                    m = {"type": "data", "session_id": self.session_id, "data": line}
                    msg = Message(data=m)
                    for exch in exchanges:
                        exch.push(msg)
                        if random.random() < self.duplicates:
                            logger.info("Sending duplicate message")
                            exch.push(msg)
                    if count % 1000 == 0:
                        logger.info(f"Sent {count} messages")
                m = {"type": EOS, "session_id": self.session_id, "id": "client"}

                for exch in exchanges:
                    exch.broadcast(Message(data=m))

            for exch in exchanges:
                exch.close()

        except Exception as e:
            logger.error(f"Exception thrown on FileProvider: {e}")
