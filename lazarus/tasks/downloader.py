import base64
import heapq

import requests

from lazarus.tasks.base import Task
from lazarus.utils import get_logger

logger = get_logger(__name__)


class BestMemeDownloader(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.top_memes = []
        self.nprocessed = 0

    def __call__(self, msg, queue_name):
        logger.debug("BestMemeDownloader :: Got %s", msg)

        if msg["url"] is None or msg["url"] == "":
            return

        if "reddit.com/r" in msg["url"]:
            return

        # Note: If mean_sentiment of two tuples is the same, then
        # the comparisson will be made with self.nprocessed
        if len(self.top_memes) >= 10:
            heapq.heappushpop(
                self.top_memes, (-float(msg["mean_sentiment"]), self.nprocessed, msg)
            )
        else:
            heapq.heappush(
                self.top_memes, (-float(msg["mean_sentiment"]), self.nprocessed, msg)
            )

        self.nprocessed += 1

    # TODO: What if there is no best meme?
    def collect(self):
        response = {}
        while True:
            try:
                _, _, meme = heapq.heappop(self.top_memes)
                res = requests.get(meme["url"])
                res.raise_for_status()
                b64 = base64.b64encode(res.content)
                response["meme"] = b64.decode("ascii")
                return [response]
            except requests.HTTPError:
                pass
            except IndexError:
                break

        return []
