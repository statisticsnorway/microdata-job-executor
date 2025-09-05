FROM ghcr.io/astral-sh/uv:bookworm-slim as builder

WORKDIR /app
COPY pyproject.toml uv.lock /app/

RUN uv python install 3.13 
RUN uv pip install --system --target=/app/dependencies .

# Create application user 
RUN groupadd --gid 180291 microdata \
    && useradd --uid 180291 --gid microdata microdata

# Production image
FROM ghcr.io/statisticsnorway/distroless-python3.13
ARG COMMIT_ID
ENV COMMIT_ID=$COMMIT_ID

WORKDIR /app
COPY job_executor job_executor

COPY --from=builder /app/dependencies /app/dependencies
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

USER microdata
ENV PYTHONPATH "${PYTHONPATH}:/app:/app/dependencies"
CMD ["job_executor/app.py"]

