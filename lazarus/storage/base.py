import abc
import json
from typing import Dict, Union, Optional
import zlib

from lazarus.exceptions import BadChecksumError

KeyType = Union[int, str]
MessageType = Union[Dict, str]
TopicType = str


class BaseStorage(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def load(cls):
        # This violates Liskov's substitution principle
        # It's mostly a reminder
        pass

    @abc.abstractmethod
    def contains(self, key: KeyType, topic: Optional[TopicType] = None):
        pass

    def __contains__(self, key):
        return self.contains(key)

    @abc.abstractmethod
    def put(
        self,
        key: KeyType,
        message: MessageType,
        topic: Optional[TopicType] = None,
        autosync: bool = True,
    ):
        pass

    @abc.abstractmethod
    def sync(self):
        pass

    @abc.abstractmethod
    def get(self, key: KeyType, topic: Optional[TopicType] = None):
        pass

    @classmethod
    def checksum(
        cls, key: KeyType, message: MessageType, topic: Optional[TopicType] = None
    ):
        full_msg = json.dumps(cls.payload(key, message, topic=topic))
        checksum = zlib.crc32(full_msg.encode())
        return checksum

    @classmethod
    def payload(
        cls, key: KeyType, message: MessageType, topic: Optional[TopicType] = None,
    ):
        return {"key": key, "message": message, "topic": topic}

    @classmethod
    def validate_message(
        cls,
        key: KeyType,
        message: MessageType,
        checksum: int,
        topic: Optional[TopicType] = None,
    ):
        new_checksum = BaseStorage.checksum(key, message, topic=topic)
        if checksum != new_checksum:
            raise BadChecksumError()
        return BaseStorage.payload(key, message, topic=topic)
