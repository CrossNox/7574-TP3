import abc
from enum import Enum
from typing import List

from lazarus.mom.message import Message
from lazarus.mom.rabbit import ExchangeType, RabbitConnection


class Exchange(RabbitConnection, abc.ABC):
    """
    This is the base class for a MOM Exchange. It provides a abstract methods that every Exchange child must follow
    """

    def __init__(self, host: str, name: str):
        super().__init__(host)
        self.exchange_name = name

    @abc.abstractmethod
    def push(self, msg: Message):
        """
        Publishes a new message into selected queues given configuration.
        - msg is the Message to publish
        """

    @abc.abstractmethod
    def broadcast(self, msg: Message):
        """
        Broadcast given message to all suscribed queues.
        - msg is the Message to publish
        """

    def close(self):
        self._close()


class BasicExchange(Exchange):
    def __init__(self, host: str, exchange_name: str, queue_name: str):
        """
        This exchange writes to a single queue which should not be shared between processes
        - host is the rabbitmq host address
        - exchange_name is the name given to this exchange, so it can be recovered
        - queue_name is the name of the queue where messages are going to be written
        """
        super().__init__(host, exchange_name)
        self.queue_name = queue_name
        self._queue_declare(queue_name=queue_name)

    def push(self, msg: Message):
        self._publish(msg, "", self.queue_name)

    def broadcast(self, msg: Message):
        self._publish(msg, "", self.queue_name)


class ConsumerType(Enum):
    """
    Type of Consumers for a WorkerExchange
    - Worker: Messages will be routed to them in a round-robin fashion (except for broadcast ones)
    - Subscriber: All messages are going to be delivered to them
    """

    Worker = 0
    Subscriber = 1


class ConsumerConfig:
    """
    Consumer configuration for a WorkerExchange
    - name: name of the consumer queue where messages should be routed
    - ctype: ConsumerType of the consumer
    """

    def __init__(self, name: str, ctype: ConsumerType):
        self.name = name
        self.type = ctype


class WorkerExchange(Exchange):
    BROADCAST = "broadcast"
    ALL_MESSAGES = "*"

    def __init__(self, host: str, exchange_name: str, consumers: List[ConsumerConfig]):
        """
        This exchange writes to multiple consumer queues, allowing a round-robin load balance configuration
        - host: rabbitmq host address
        - exchange_name: name given to this exchange, so it can be recovered
        - consumers: a list of all the consumers where messages should be routed
        """
        super().__init__(host, exchange_name)
        self.consumers: List[str] = []
        self.count = 0
        self.n_workers = 0

        if len(consumers) == 0:
            raise ValueError(
                "A WorkerExchange must have at least one consumer, received zero"
            )

        self._exchange_declare(
            exchange_name=exchange_name, exchange_type=ExchangeType.Topic
        )

        for c in consumers:
            self.consumers.append(c.name)
            self._queue_declare(c.name)
            if c.type == ConsumerType.Worker:
                self._queue_bind(c.name, exchange_name, binding_key=f"{self.n_workers}")
                self._queue_bind(c.name, exchange_name, binding_key=self.BROADCAST)
                self.n_workers += 1
            else:
                self._queue_bind(c.name, exchange_name, binding_key=self.ALL_MESSAGES)

    def push(self, msg: Message):
        self._publish(msg, self.exchange_name, routing_key=f"{self.count}")
        if self.n_workers != 0:
            self.count = (self.count + 1) % self.n_workers

    def broadcast(self, msg: Message):
        self._publish(msg, self.exchange_name, routing_key=self.BROADCAST)
