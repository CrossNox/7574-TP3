FROM python:3.8-slim
RUN apt update
RUN apt install -y graphviz
RUN pip install poetry

WORKDIR /app
COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock
COPY README.md README.md
RUN poetry install --no-dev --no-root
RUN poetry install

COPY lazarus lazarus
COPY tests tests

ENTRYPOINT ["poetry", "run", "mom_test", "-vv"]