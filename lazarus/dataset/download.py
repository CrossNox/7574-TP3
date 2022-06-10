import os
import pathlib
from contextlib import contextmanager

import pandas as pd

from lazarus.utils import get_logger
from lazarus.constants import POSTS_FILENAME, COMMENTS_FILENAME, DEFAULT_SAMPLE_SIZE

logger = get_logger(__name__)


@contextmanager
def environment_variable(key, value):
    prev_value = os.getenv(key)
    os.environ[key] = str(value)
    try:
        yield
    finally:
        if prev_value is not None:
            os.environ[key] = prev_value
        else:
            del os.environ[key]


def download_dataset(
    kaggle_json_loc: pathlib.Path,
    outputdir: pathlib.Path,
    sample_size: float = DEFAULT_SAMPLE_SIZE,
):
    if outputdir.exists() and not outputdir.is_dir():
        raise ValueError("The output dir should be a directory, not a file")
    if not kaggle_json_loc.is_dir():
        raise ValueError(
            "The kaggle json location should be a directory with a kaggle.json file in it, not a file"
        )

    with environment_variable("KAGGLE_CONFIG_DIR", kaggle_json_loc):

        logger.info("Authenticating kaggle API with file on %s", kaggle_json_loc)
        from kaggle import KaggleApi

        api = KaggleApi()
        api.authenticate()

        if (
            not pathlib.Path(outputdir / COMMENTS_FILENAME).exists()
            or not pathlib.Path(outputdir / POSTS_FILENAME).exists()
        ):
            logger.info("Downloading data to %s", outputdir)
            api.dataset_download_files(
                "pavellexyr/the-reddit-irl-dataset",
                path=outputdir,
                quiet=False,
                unzip=True,
            )
        else:
            logger.info("Data already downloaded")

        reduced_posts_out = (
            outputdir / f"{pathlib.Path(outputdir / POSTS_FILENAME).stem}-reduced.csv"
        )
        logger.info("Saving reduced posts dataset to %s", reduced_posts_out)
        df_posts = pd.read_csv(outputdir / POSTS_FILENAME)
        n_posts = int(len(df_posts) * sample_size)
        df_posts.head(n_posts).to_csv(reduced_posts_out, index=False)

        reduced_comments_out = (
            outputdir
            / f"{pathlib.Path(outputdir / COMMENTS_FILENAME).stem}-reduced.csv"
        )
        logger.info("Saving reduced comments dataset to %s", reduced_comments_out)
        df_comments = pd.read_csv(outputdir / COMMENTS_FILENAME)
        n_comments = int(len(df_comments) * sample_size)
        df_comments.head(n_comments).to_csv(
            reduced_comments_out,
            index=False,
        )

        logger.info("All data downloaded")
