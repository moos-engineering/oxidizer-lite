FROM python:3.14-slim-trixie AS builder

ARG SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0.dev0

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY pyproject.toml README.md ./
COPY docs ./docs
COPY oxidizer ./oxidizer

RUN pip install --no-cache-dir --prefix=/install ".[s3,redis,kafka,duckdb,anvil]"

FROM python:3.14-slim-trixie AS base

WORKDIR /oxidizer
ENV PYTHONPATH=/oxidizer

COPY --from=builder /install/ /usr/local/
COPY docker/controller.py docker/worker.py /oxidizer/app/
COPY docker/__init__.py /oxidizer/app/__init__.py
COPY docker/utils/__init__.py /oxidizer/app/utils/__init__.py
COPY docker/utils/helpers.py /oxidizer/app/utils/helpers.py

FROM base AS api-builder
COPY --from=builder /build /build
ARG SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0.dev0
RUN pip install --no-cache-dir "/build[mcp]" \
    && python -m oxidizer.utils.chisel

FROM base AS api
COPY --from=api-builder /usr/local/ /usr/local/
COPY --from=api-builder /opt/oxidizer/search /opt/oxidizer/search
COPY docker/api.py /oxidizer/app/api.py
ENV HF_HUB_OFFLINE=1 HF_HUB_DISABLE_TELEMETRY=1 TOKENIZERS_PARALLELISM=false
EXPOSE 8000
CMD ["python", "-m", "app.api"]

FROM base AS runtime