import json
from pathlib import Path
from io import TextIOWrapper
from typing import Dict, Optional
from contextlib import contextmanager

from lazarus.utils import get_logger
from lazarus.exceptions import BadChecksumError
from lazarus.storage.base import KeyType, TopicType, BaseStorage, MessageType

logger = get_logger(__name__)


class LocalStorage(BaseStorage):
    @classmethod
    def load(cls, data_folder: Path):  # pylint: disable=arguments-differ
        logger.info("Loading storage from %s", data_folder)
        files_to_load = False
        if data_folder.exists():
            files_to_load = True

        storage = LocalStorage(data_folder)

        if not files_to_load:
            return storage

        for file in data_folder.glob("*.jsonl"):
            file = file.replace(file.with_suffix(".jsonl.backup"))
            with open(file) as f:
                for line in f:
                    line = line.strip("\n")

                    if line == "":
                        continue

                    json_line = json.loads(line)

                    try:
                        storage.put(**BaseStorage.validate_message(**json_line))
                    except BadChecksumError:
                        logger.error("Line %s does not match checksum", json_line)

        return storage

    def __init__(self, data_folder: Path):
        self.data_dir = data_folder
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.data: Dict[TopicType, Dict[KeyType, MessageType]] = dict()
        self.data_files: Dict[TopicType, TextIOWrapper] = dict()
        self._recovery_mode = False  # TODO: move this to the base class
        logger.info("New storage from %s", self.data_dir)

    @property
    def in_recovery_mode(self):
        return self._recovery_mode

    @contextmanager
    def recovery_mode(self):
        """Recovery mode ensures no changes to the storage will be made
        while allowing to fetch data from it."""
        # Move to base class
        self._recovery_mode = True
        yield self
        self._recovery_mode = False

    def __del__(self):
        for v in self.data_files.values():
            v.close()

    def contains(self, key: KeyType, topic: Optional[TopicType] = None):
        try:
            return key in self.data[topic]
        except KeyError:
            return False

    def contains_topic(self, topic: TopicType):
        return topic in self.data

    def put(
        self,
        key: KeyType,
        message: MessageType,
        topic: Optional[TopicType] = None,
        autosync: bool = True,
    ):
        if self._recovery_mode:
            return

        if topic not in self.data_files:
            self.data_files[topic] = open(self.data_dir / f"{topic}.jsonl", "w")

        self.data_files[topic].write(
            json.dumps(
                {
                    **BaseStorage.payload(key, message, topic),
                    "checksum": self.checksum(key, message, topic=topic),
                }
            )
        )
        self.data_files[topic].write("\n")

        if topic not in self.data:
            self.data[topic] = {}

        self.data[topic][key] = message

        if autosync:
            self.sync(topic)

    def iter_topic(self, topic: TopicType):
        if topic not in self.data:
            raise KeyError(f"Topic {topic} is not present")
        for k, v in self.data[topic].items():
            yield k, v

    def sync(self, topic: Optional[TopicType] = None):
        if self._recovery_mode:
            return

        if topic is not None:
            self.data_files[topic].flush()
        else:
            for f in self.data_files.values():
                f.flush()

    def get(self, key: KeyType, topic: Optional[TopicType] = None):
        return self.data[topic][key]
