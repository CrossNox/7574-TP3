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
    def _execute(self, message):
        message_id = re.match(
            r"https://old.reddit.com/r/me_?irl/comments/([^/]+)/.*",
            message["permalink"],
        ).groups()
        if message_id is None:
            logger.error("Bad permalink %s", message["permalink"])
            return
        message["id"] = message_id[0]
        return message


class PostsMeanSentiment(Transform):
    def __init__(self):
        super().__init__()
        self.sentiment_scores = defaultdict(lambda: {"count": 0, "sum": 0})

    def _execute(self, message):
        post_id = message["id"]
        self.sentiment_scores[post_id]["count"] += 1
        self.sentiment_scores[post_id]["sum"] += float(message["sentiment"])

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

    def _execute(self, message):
        self.n_posts_scores += 1
        self.total_posts_scores += float(message["score"])

    def collect(self):
        return [{"posts_mean_score": self.total_posts_scores / self.n_posts_scores}]

    def reset(self):
        self.n_posts_scores = 0
        self.total_posts_scores = 0


class FilterColumn(Transform):
    def __init__(self, columns: List[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = set(columns)

    def _execute(self, message):
        message = {k: v for k, v in message.items() if k in self.columns}
        return message
