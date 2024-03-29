import queue as q
from multiprocessing import Event

from lazarus.cfg import cfg
from lazarus.mom.queue import Queue
from lazarus.utils import get_logger
from lazarus.mom.message import Message
from lazarus.constants import EOS, DEFAULT_MOM_HOST

logger = get_logger(__name__)


MOM_HOST: str = cfg.mom_host(default=DEFAULT_MOM_HOST)


class ResultCollector:
    def __init__(self, queue_name: str):
        # Deberá tener información suficiente como para levantar una cola sobre la cual consumir
        self.input_queue = None
        self.queue_name = queue_name
        self.result_queue: q.Queue = q.Queue(maxsize=1)
        self.running = False
        self.msg_collected = Event()

    # Starts consuming results. Must be idempotent
    def start(self):
        if self.running:
            return

        self.input_queue = Queue(MOM_HOST, self.queue_name)

        def callback(msg: Message):
            if msg["type"] == EOS:
                msg.ack()
                return

            self.result_queue.put(msg)
            self.msg_collected.wait()
            self.msg_collected.clear()
            msg.ack()

        self.input_queue.consume(callback)
        self.running = True

    # Stops consuming results. Must be idempotent
    def stop(self):
        if not self.running:
            return

        if self.input_queue is not None:
            self.input_queue.close()
            self.input_queue = None

        self.running = False

    # Resturns a result or None.
    # If the msg is from another session id, it will be ignored
    def try_get_result(self, session_id: int):
        res = None

        try:
            res = self.result_queue.get(block=False)
        except Exception:
            return None

        if "session_id" not in res:
            logger.error("Caution, received msg without session id on server collector")
            self.ack()
            return None

        sess_id = int(res["session_id"])

        if sess_id != session_id:
            logger.info(
                f"Received an old result of session id {sess_id} on server collector"
            )
            self.ack()
            return None

        return self.__format_result(res)

    def ack(self):
        self.msg_collected.set()

    def __format_result(self, res):
        # Here we should format data to something like this
        data = res["data"]
        return {
            "posts_score_avg": data["average_posts_score"],
            "best_meme": data["best_meme_download"],
            "education_memes": data["education_joiner"],
        }
