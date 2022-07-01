import abc
from typing import Dict, Union, Optional

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

    def __in__(self, key):
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
