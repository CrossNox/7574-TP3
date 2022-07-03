import abc
from enum import Enum
from typing import Optional

from lazarus.mom.message import Message
from lazarus.utils import DEFAULT_PRETTY, DEFAULT_VERBOSE, get_logger, config_logging
import pika

logger = get_logger(__name__)


class ExchangeType(Enum):
    Topic = "topic"
    Fanout = "fanout"


class RabbitConnection(abc.ABC):
    def __init__(self, host: str):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()

    @abc.abstractmethod
    def close(self):
        """
        Should be called in order to properly close the connection and free all resources
        """

    def _close(self):
        self.channel.close()
        self.connection.close()

    def _queue_declare(self, queue_name: str):
        self.channel.queue_declare(
            queue=queue_name, durable=True, exclusive=False, auto_delete=False
        )

    def _exchange_declare(self, exchange_name: str, exchange_type: ExchangeType):
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type.value,
            durable=True,  # Not sure if this is necessary
            auto_delete=False,
            internal=False,
        )

    def _queue_bind(
        self, queue_name: str, exchange_name: str, binding_key: Optional[str] = None
    ):
        self.channel.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=binding_key
        )

    def _publish(self, msg: Message, exchange: str, routing_key: str):
        try:
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=msg.encode(),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent  # Not sure about this
                ),
                mandatory=True,  # Message must be routable to at least one queue
            )
        except pika.exceptions.UnroutableError:  # pylint: disable=no-member
            logger.error(
                f"Unroutable message {msg} to exchange {exchange} with routing key {routing_key}"
            )
            raise
