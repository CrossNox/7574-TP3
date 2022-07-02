import typer

from lazarus.utils import get_logger

logger = get_logger(__name__)

app = typer.Typer()


@app.command()
def dummy():
    pass
