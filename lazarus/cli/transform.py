from typing import List

import typer

from lazarus.nodes.node import Node
from lazarus.utils import get_logger
from lazarus.tasks.transforms import (
    FilterColumn,
    ExtractPostID,
    PostsMeanScore,
    PostsMeanSentiment,
)

logger = get_logger(__name__)

app = typer.Typer()


@app.command()
def extract_post_id():
    node = Node(callback=ExtractPostID)
    node.start()


@app.command()
def posts_mean_sentiment():
    node = Node(callback=PostsMeanSentiment)
    node.start()


@app.command()
def posts_mean_score():
    node = Node(callback=PostsMeanScore)
    node.start()


@app.command()
def filter_columns(
    columns: List[str] = typer.Argument(..., help="The list of columns to keep")
):
    node = Node(callback=FilterColumn, columns=columns)
    node.start()
