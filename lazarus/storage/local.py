import json
from pathlib import Path
from io import TextIOWrapper
from typing import Dict, Optional
from collections import defaultdict

from lazarus.storage.base import KeyType, TopicType, BaseStorage, MessageType


class LocalStorage(BaseStorage):
    @classmethod
    def load(cls, data_folder: Path):  # pylint: disable=arguments-differ
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
                    print(line)
                    storage.put(**json.loads(line))

        return storage

    def __init__(self, data_folder: Path):
        self.data_dir = data_folder
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.data: Dict[TopicType, Dict[KeyType, MessageType]] = defaultdict(dict)
        self.data_files: Dict[TopicType, TextIOWrapper] = {}

    def __del__(self):
        for v in self.data_files.values():
            v.close()

    def contains(self, key: KeyType, topic: Optional[TopicType] = None):
        return key in self.data[topic]

    def put(
        self,
        key: KeyType,
        message: MessageType,
        topic: Optional[TopicType] = None,
        autosync: bool = True,
    ):
        if topic not in self.data_files:
            self.data_files[topic] = open(self.data_dir / f"{topic}.jsonl", "w")

        self.data_files[topic].write(
            json.dumps({"topic": topic, "key": key, "message": message})
        )
        self.data_files[topic].write("\n")

        self.data[topic][key] = message

        if autosync:
            self.sync(topic)

    def sync(self, topic: Optional[TopicType] = None):
        if topic is not None:
            self.data_files[topic].flush()
        else:
            for f in self.data_files.values():
                f.flush()

    def get(self, key: KeyType, topic: Optional[TopicType] = None):
        return self.data[topic][key]
