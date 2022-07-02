FROM python:3.8-slim
RUN apt update
RUN pip install poetry
WORKDIR /app
COPY pyproject.toml /app/pyproject.toml
COPY poetry.lock /app/poetry.lock
COPY README.md /app/README.md
RUN poetry install --no-dev --no-root
COPY lazarus /app/lazarus
RUN poetry install
ENTRYPOINT ["poetry", "run", "lazarus", "-vv"]
