import pathlib
from typing import Dict, List

KAGGLE_FOLDER = pathlib.Path.home() / ".kaggle"
DATA_FOLDER = pathlib.Path(__name__).parent.parent / "data"
POSTS_FILENAME = "the-reddit-irl-dataset-posts.csv"
COMMENTS_FILENAME = "the-reddit-irl-dataset-comments.csv"

DEFAULT_SAMPLE_SIZE: float = 0.01

ED_KWDS: List[str] = ["university", "college", "student", "teacher", "professor"]
ED_KWDS_PATTERN: str = f'({"|".join(ED_KWDS)})'

EOS: Dict = {"type": "EOS"}

DEFAULT_SLEEP_TIME: int = 10
DEFAULT_HEARTBEAT_PORT: int = 8080
DEFAULT_PING_PORT: int = 8080
HEARTBEAT: str = "HEARTBEAT"
PING: str = "PING"

DOCKER_IMAGE_NAME: str = "7574-tp3:latest"
DOCKER_NETWORK: str = "lazarus_net"

EPSILON: float = 0.05
