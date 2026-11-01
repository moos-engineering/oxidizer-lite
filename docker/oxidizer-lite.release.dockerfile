FROM python:3.14-slim-trixie AS builder

ARG OXIDIZER_VERSION

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN test -n "$OXIDIZER_VERSION" \
    && pip install --no-cache-dir --retries 10 --prefix=/install \
    "oxidizer-lite[s3,redis,kafka,duckdb,anvil]==${OXIDIZER_VERSION}"

FROM python:3.14-slim-trixie AS base

ARG OXIDIZER_VERSION

WORKDIR /oxidizer
ENV PYTHONPATH=/oxidizer

COPY --from=builder /install/ /usr/local/
COPY docker/controller.py docker/worker.py /oxidizer/app/
COPY docker/__init__.py /oxidizer/app/__init__.py
COPY docker/utils/__init__.py /oxidizer/app/utils/__init__.py
COPY docker/utils/helpers.py /oxidizer/app/utils/helpers.py

RUN OXIDIZER_EXPECTED_VERSION="$OXIDIZER_VERSION" python -c \
    'import os; from importlib.metadata import version; import oxidizer; assert version("oxidizer-lite") == os.environ["OXIDIZER_EXPECTED_VERSION"]'

LABEL org.opencontainers.image.version="${OXIDIZER_VERSION}"

FROM base AS api-builder
ARG OXIDIZER_VERSION
RUN pip install --no-cache-dir --retries 10 \
    "oxidizer-lite[mcp]==${OXIDIZER_VERSION}" \
    && python -m oxidizer.utils.chisel

FROM base AS api
COPY --from=api-builder /usr/local/ /usr/local/
COPY --from=api-builder /opt/oxidizer/search /opt/oxidizer/search
COPY docker/api.py /oxidizer/app/api.py
ENV HF_HUB_OFFLINE=1 HF_HUB_DISABLE_TELEMETRY=1 TOKENIZERS_PARALLELISM=false
EXPOSE 8000
CMD ["python", "-m", "app.api"]

FROM base AS runtime