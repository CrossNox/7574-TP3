from typing import Callable

from mom.message import Message

from lazarus.cfg import cfg
from lazarus.utils import get_logger
from lazarus.mom.rabbit import RabbitConnection

logger = get_logger(__name__)


class Queue(RabbitConnection):
    def __init__(self, host: str, queue_name: str):
        """
        A MOM persistent queue abstraction, which can be used to receive messages
        - host: rabbitmq host address
        - queue_name: name given to this queue, so it can be recovered
        """
        super().__init__(host)
        self.queue_name = queue_name
        self._queue_declare(queue_name)
        self.channel.basic_qos(prefetch_count=cfg.mom_prefetch_count(default=10))

    def consume(self, callback: Callable[[Message], None]):
        """
        Sets a new callback to call when a new message arrives
        """

        def __callback(ch, method, _, body: bytes):
            msg = Message.decode(body)
            # Dynamically set ack and nack methods to message
            msg.ack = lambda: ch.basic_ack(delibery_tag=method.delivery_tag)
            msg.nack = lambda: ch.basic_nack(delibery_tag=method.delivery_tag)
            callback(msg)

        self.channel.basic_consume(self.queue_name, __callback, auto_ack=False)
