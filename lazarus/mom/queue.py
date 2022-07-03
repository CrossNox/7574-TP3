from threading import Event, Thread
from typing import Callable

from lazarus.mom.message import Message
from lazarus.mom.rabbit import RabbitConnection
from lazarus.utils import get_logger

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
        self.tag = ""
        self.channel.basic_qos(prefetch_count=1)  # TODO: Get from config
        self.worker = Thread(target=lambda: None)

    def __str__(self):
        return f"Queue {self.queue_name}"

    def __work(self, callback):
        try:
            self.tag = self.channel.basic_consume(
                self.queue_name,
                on_message_callback=callback,
                consumer_tag=self.tag,
                auto_ack=False,
            )
            self.channel.start_consuming()
        except Exception as e:
            logger.error(
                "Exception captured on Queue consumer thread: %s", e, exc_info=True
            )
            raise

    def consume(self, callback: Callable[[Message], None]):
        """
        Starts consuming queue in a new thread with given callback as handler
        """

        def __callback(ch, method, _, body: bytes):
            msg = Message.decode(body)
            # Dynamically set ack and nack methods to message
            msg.ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            msg.nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            callback(msg)

        self.worker = Thread(target=self.__work, args=[__callback])
        self.worker.start()

    def close(self):

        closed = Event()  # This will help us signal callback execution

        def __close_callback():
            self.channel.basic_cancel(self.tag)
            self._close()
            closed.set()

        self.connection.add_callback_threadsafe(__close_callback)

        closed.wait()  # Wait for callback to execute
        self.worker.join()  # Join consumer thread
