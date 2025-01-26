# BUILDER STAGE
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

# Build the virtual environment
COPY . .
RUN uv sync --frozen --no-cache

# Change symlinks in the virtual environment to point to
# the correct Python binary of the final distroless-based runtime image
RUN ln -sf /usr/bin/python /.venv/bin/python

# Pre-create directories and change ownership to the non-root user to avoid permission issues
RUN mkdir -p /app/.chainlit
RUN mkdir -p /app/.files
RUN chown -R 65532:65532 /app

# RUNTIME IMAGE
FROM gcr.io/distroless/python3-debian12:nonroot AS runtime

# Copy from the builder stage to the runtime image
COPY --from=builder /.venv /.venv
COPY --from=builder /app /app

# Set the working directory
WORKDIR /app

ENTRYPOINT ["/.venv/bin/python3"]

CMD ["/.venv/bin/chainlit", "run", "main.py"]
