import typer

from lazarus.nodes.node import Node
from lazarus.utils import get_logger
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
    node = Node(callback=FilterUniqIDs)
    node.start()


@app.command()
def posts_score_above_mean(
    mean_host: str = typer.Argument(
        ..., help="Address where to fetch the mean score for posts from"
    )
):
    node = Node(callback=FilterPostsScoreAboveMean, dependencies={"mean": mean_host})
    node.start()


@app.command()
def ed_comments():
    node = Node(callback=FilterEdComment)
    node.start()


@app.command()
def nan_sentiment():
    node = Node(callback=FilterNanSentiment)
    node.start()


@app.command()
def null_url():
    node = Node(callback=FilterNullURL)
    node.start()
