from collections import defaultdict
import re
from typing import List

from lazarus.tasks.base import Task
from lazarus.utils import get_logger

logger = get_logger(__name__)


class Transform(Task):
    def collect(self):
        return None


class CommentFilter(Transform):
    def __init__(self, columns: List[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = set(columns)

    def __call__(self, msg, queue_name):
        try:
            float(msg["sentiment"])
        except:  # pylint: disable=bare-except
            logger.error("Bad sentiment %s", msg["sentiment"])
            return None

        msg_id = re.match(
            r"https://old.reddit.com/r/me_?irl/comments/([^/]+)/.*", msg["permalink"]
        ).groups()
        if msg_id is None:
            logger.error("Bad permalink %s", msg["permalink"])
            return

        msg["id"] = msg_id[0]
        msg = {k: v for k, v in msg.items() if k in self.columns}

        return msg


class PostsMeanSentiment(Transform):
    def __init__(self):
        super().__init__()
        self.sentiment_scores = defaultdict(lambda: {"count": 0, "sum": 0})

    def __call__(self, msg, queue_name):
        post_id = msg["id"]
        self.sentiment_scores[post_id]["count"] += 1
        self.sentiment_scores[post_id]["sum"] += float(msg["sentiment"])

    def collect(self):
        return [
            {"id": k, "mean_sentiment": v["sum"] / v["count"]}
            for k, v in self.sentiment_scores.items()
        ]

    def reset(self):
        self.sentiment_scores = defaultdict(lambda: {"count": 0, "sum": 0})


class PostsMeanScore(Transform):
    def __init__(self):
        super().__init__()
        self.n_posts_scores = 0
        self.total_posts_scores = 0

    def __call__(self, msg, queue_name):
        self.n_posts_scores += 1
        self.total_posts_scores += float(msg["score"])

    def collect(self):
        return [
            {
                "posts_mean_score": self.total_posts_scores / self.n_posts_scores,
                "id": "posts_mean_score",
            }
        ]

    def reset(self):
        self.n_posts_scores = 0
        self.total_posts_scores = 0


class FilterColumn(Transform):
    def __init__(self, columns: List[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = set(columns)

    def __call__(self, msg, queue_name):
        msg = {k: v for k, v in msg.items() if k in self.columns}
        return msg
