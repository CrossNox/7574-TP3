FROM python:3.8-slim
RUN apt update
RUN pip install poetry
COPY pyproject.toml /app/pyproject.toml
COPY poetry.lock /app/poetry.lock
COPY README.md /app/README.md
COPY lazarus /app/lazarus
WORKDIR /app
RUN poetry install --no-dev --no-root
RUN poetry install
ENTRYPOINT ["poetry", "run", "lazarus", "-vv"]
