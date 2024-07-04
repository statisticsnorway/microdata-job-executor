# Export Poetry Packages
FROM python:3.12-bookworm as builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.7.1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

# Prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

# Install tools
RUN apt-get update \
    && apt-get install -y  --no-install-recommends \
    ca-certificates \
    curl \
    build-essential \
    python3-distutils \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

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
FROM python:3.13.0b2-slim

# Create user
RUN groupadd --gid 180291 microdata \
  && useradd --uid 180291 --gid microdata microdata

WORKDIR /app
COPY job_executor job_executor
#To use application version in logs
COPY --from=builder /app/pyproject.toml pyproject.toml
COPY --from=builder /app/requirements.txt requirements.txt

RUN pip install -r requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/app"

# Change user
USER microdata

CMD [ "python", "job_executor/app.py"]

