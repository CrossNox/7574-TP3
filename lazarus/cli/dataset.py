import pathlib

import typer

from lazarus.cfg import cfg
from lazarus.utils import path_or_none
from lazarus.dataset.download import download_dataset
from lazarus.constants import DATA_FOLDER, KAGGLE_FOLDER, DEFAULT_SAMPLE_SIZE

app = typer.Typer()


@app.command()
def download(
    kaggle_json_loc: pathlib.Path = typer.Argument(
        cfg.kaggle.json_loc(default=KAGGLE_FOLDER, cast=path_or_none),
        help="Path to the folder containing the json with the Kaggle credentials",
    ),
    outputdir: pathlib.Path = typer.Argument(
        cfg.data.outputdir(default=DATA_FOLDER, cast=path_or_none),
        help="Path where to save data to",
    ),
    sample_size: float = typer.Option(
        DEFAULT_SAMPLE_SIZE, help="Sample size for reduced datasets", min=0.001, max=1.0
    ),
):
    download_dataset(
        kaggle_json_loc=kaggle_json_loc, outputdir=outputdir, sample_size=sample_size
    )
