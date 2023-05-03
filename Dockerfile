# Export Poetry Packages
FROM ubuntu:22.04 as builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.4.1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

# Prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

# Install python 3.10
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    python3.10 \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3.10 /usr/bin/python

# Install tools
RUN apt-get update \
    && apt-get install -y  --no-install-recommends \
    ca-certificates \
    curl \
    build-essential \
    python3-distutils \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY poetry.lock pyproject.toml /app/

# Install poetry and export dependencies to requirements yaml
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl -sSL https://install.python-poetry.org | python3 - --version $POETRY_VERSION

#Set application version in pyproject.toml, use zero if not set
ARG BUILD_NUMBER=0
RUN poetryVersion=$(poetry version -s); buildNumber=${BUILD_NUMBER}; newVersion=$(echo $poetryVersion | sed "s/[[:digit:]]\{1,\}$/$buildNumber/"); poetry version $newVersion

RUN poetry export > requirements.txt

# Production image
FROM python:3.10-slim

WORKDIR /app
COPY job_executor job_executor
#To use application version in logs
COPY --from=builder /app/pyproject.toml pyproject.toml
COPY --from=builder /app/requirements.txt requirements.txt

RUN pip install -r requirements.txt

#the output is sent straight to terminal without being first buffered
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD [ "python", "job_executor/app.py"]

