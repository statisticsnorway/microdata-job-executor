# Export Poetry Packages
FROM python:3.12-bookworm AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.7.1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

# Prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$PATH"

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
RUN curl --proto '=https' --tlsv1.2 -sSL https://install.python-poetry.org | python3 - --version "$POETRY_VERSION"

RUN poetry export > requirements.txt

# Production image
FROM python:3.12-slim
ARG COMMIT_ID
ENV COMMIT_ID=$COMMIT_ID
RUN echo "Commit ID: $COMMIT_ID"

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

