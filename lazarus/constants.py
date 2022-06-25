import pathlib
from typing import List

KAGGLE_FOLDER = pathlib.Path.home() / ".kaggle"
DATA_FOLDER = pathlib.Path(__name__).parent.parent / "data"
POSTS_FILENAME = "the-reddit-irl-dataset-posts.csv"
COMMENTS_FILENAME = "the-reddit-irl-dataset-comments.csv"

DEFAULT_SAMPLE_SIZE: float = 0.01

ED_KWDS: List[str] = ["university", "college", "student", "teacher", "professor"]
ED_KWDS_PATTERN: str = f'({"|".join(ED_KWDS)})'

DEFAULT_SLEEP_TIME: int = 30
