FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=/app/.venv

COPY pyproject.toml uv.lock ./

RUN apt-get update \
    && apt-get install -y curl build-essential pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/* \
    && curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain stable

ENV PATH="/root/.cargo/bin:/app/.venv/bin:${PATH}"
ENV UV_INDEX_STRATEGY=unsafe-best-match

RUN uv sync --frozen --no-dev

RUN uv add torch torchvision \
    --index https://download.pytorch.org/whl/cpu \
    --index https://pypi.org/simple \
    --index-strategy unsafe-best-match \
    --no-sync

COPY . .

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]