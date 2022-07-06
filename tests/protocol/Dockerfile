# FROM python:3.8-slim
# RUN apt update
# RUN pip install poetry

# WORKDIR /app
# COPY pyproject.toml /app/pyproject.toml
# COPY poetry.lock /app/poetry.lock
# COPY README.md /app/README.md
# RUN poetry install --no-dev --no-root

# COPY lazarus /app/lazarus
# COPY tests tests
# RUN poetry install

FROM python:3.8-slim
RUN apt update
RUN pip install poetry

WORKDIR /app
COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock
COPY README.md README.md
RUN poetry install --no-dev --no-root
RUN poetry install

COPY lazarus lazarus
COPY tests tests


ENTRYPOINT ["poetry", "run", "protocol_test", "-vv"]
