import re
from typing import List
from collections import defaultdict

from lazarus.tasks.base import Task
from lazarus.utils import get_logger

logger = get_logger(__name__)


class Transform(Task):
    def collect(self):
        return None


class ExtractPostID(Transform):
    def __call__(self, msg):
        msg_id = re.match(
            r"https://old.reddit.com/r/me_?irl/comments/([^/]+)/.*", msg["permalink"]
        ).groups()
        if msg_id is None:
            logger.error("Bad permalink %s", msg["permalink"])
            return
        msg["id"] = msg_id[0]
        return msg


class PostsMeanSentiment(Transform):
    def __init__(self):
        super().__init__()
        self.sentiment_scores = defaultdict(lambda: {"count": 0, "sum": 0})

    def __call__(self, msg):
        post_id = msg["id"]
        self.sentiment_scores[post_id]["count"] += 1
        self.sentiment_scores[post_id]["sum"] += float(msg["sentiment"])

    def collect(self):
        return [
            {"id": k, "mean_sentiment": v["sum"] / v["count"]}
            for k, v in self.sentiment_scores.items()
        ]


class PostsMeanScore(Transform):
    def __init__(self):
        super().__init__()
        self.n_posts_scores = 0
        self.total_posts_scores = 0

    def handle_msg(self, msg):
        self.n_posts_scores += 1
        self.total_posts_scores += float(msg["score"])

    def collect(self):
        return [{"posts_mean_score": self.total_posts_scores / self.n_posts_scores}]


class FilterColumn(Transform):
    def __init__(self, columns: List[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = set(columns)

    def handle_msg(self, msg):
        msg = {k: v for k, v in msg.items() if k in self.columns}
        return msg
