import re

from lazarus.tasks.base import Task
from lazarus.utils import get_logger
from lazarus.constants import ED_KWDS_PATTERN

logger = get_logger(__name__)


class Filter(Task):
    def collect(self):
        return None


class FilterPostsScoreAboveMean(Filter):
    def __init__(self, posts_mean_score):
        super().__init__()
        self.mean = posts_mean_score

    def __call__(self, message):
        msg_score = float(message["score"])  # TODO: WHY is this a string
        if msg_score >= self.mean:
            return message


class FilterEdComment(Filter):
    def __call__(self, message):
        msg_body = message["body"].lower()
        if re.search(ED_KWDS_PATTERN, msg_body) is not None:
            return message


class FilterNanSentiment(Filter):
    def __call__(self, message):
        try:
            float(message["sentiment"])
            return message
        except:  # pylint: disable=bare-except
            pass


class FilterNullURL(Filter):
    def __call__(self, message):
        message_url = message["url"]
        if message_url is not None and message_url != "":
            return message


class FilterUniqIDs(Filter):
    def __init__(self):
        super().__init__()
        self.ids = set()

    def collect(self):
        return list(self.ids)

    def __call__(self, message):
        new_id = message["id"]
        if new_id is not None:
            self.ids.add(new_id)
