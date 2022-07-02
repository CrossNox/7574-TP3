import typer

from lazarus.nodes.node import Node
from lazarus.utils import get_logger
from lazarus.sidecar import HeartbeatSender
from lazarus.tasks.filters import (
    FilterNullURL,
    FilterUniqIDs,
    FilterEdComment,
    FilterNanSentiment,
    FilterPostsScoreAboveMean,
)

logger = get_logger(__name__)

app = typer.Typer()


@app.command()
def uniq_posts():
    heartbeat_sender = HeartbeatSender()
    heartbeat_sender.start()

    node = Node(callback=FilterUniqIDs)
    node.start()


@app.command()
def posts_score_above_mean(
    mean_host: str = typer.Argument(
        ..., help="Address where to fetch the mean score for posts from"
    )
):
    heartbeat_sender = HeartbeatSender()
    heartbeat_sender.start()

    node = Node(callback=FilterPostsScoreAboveMean, dependencies={"mean": mean_host})
    node.start()


@app.command()
def ed_comments():
    heartbeat_sender = HeartbeatSender()
    heartbeat_sender.start()

    node = Node(callback=FilterEdComment)
    node.start()


@app.command()
def nan_sentiment():
    heartbeat_sender = HeartbeatSender()
    heartbeat_sender.start()

    node = Node(callback=FilterNanSentiment)
    node.start()


@app.command()
def null_url():
    heartbeat_sender = HeartbeatSender()
    heartbeat_sender.start()

    node = Node(callback=FilterNullURL)
    node.start()
