import pathlib
from typing import List

KAGGLE_FOLDER = pathlib.Path.home() / ".kaggle"
DATA_FOLDER = pathlib.Path(__name__).parent.parent / "data"
POSTS_FILENAME = "the-reddit-irl-dataset-posts.csv"
COMMENTS_FILENAME = "the-reddit-irl-dataset-comments.csv"

DEFAULT_SAMPLE_SIZE: float = 0.01

ED_KWDS: List[str] = ["university", "college", "student", "teacher", "professor"]
ED_KWDS_PATTERN: str = f'({"|".join(ED_KWDS)})'

EOS: str = "EOS"

DEFAULT_SLEEP_TIME: int = 2
DEFAULT_HEARTBEAT_PORT: int = 8080
DEFAULT_PING_PORT: int = 8080
HEARTBEAT: str = "HEARTBEAT"
PING: str = "PING"

DOCKER_IMAGE_NAME: str = "7574-tp3:latest"
DOCKER_NETWORK: str = "lazarus_net"

EPSILON: float = 0.05

NO_SESSION: int = -1

DEFAULT_SERVER_PORT: int = 8000
DEFAULT_PROTOCOL_RETRY_SLEEP: int = 5
DEFAULT_PROTOCOL_TIMEOUT: int = 5

DEFAULT_MOM_HOST: str = "rabbitmq"
DEFAULT_POSTS_EXCHANGE: str = "posts"
DEFAULT_COMMENTS_EXCHANGE: str = "comments"

DEFAULT_SERVER_DB_EXCHANGE: str = "serverdb"
DEFAULT_SERVER_DB_TOPIC: str = "serverdb"
