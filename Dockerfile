# Airbyte source connector for Orca Scan.
#
#   docker build -t airbyte/source-orca-scan:dev .
#   docker run --rm airbyte/source-orca-scan:dev spec
#   docker run --rm -v "$PWD/secrets:/secrets" airbyte/source-orca-scan:dev check --config /secrets/config.json

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUTF8=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"

WORKDIR /airbyte/integration_code

# Dependencies first so they cache independently of the connector source.
COPY pyproject.toml README.md ./
COPY source_orca_scan ./source_orca_scan
COPY main.py ./
RUN pip install . \
    && rm -rf /root/.cache

# Airbyte runs connectors as a non-root user.
RUN useradd --create-home --uid 1000 airbyte \
    && chown -R airbyte:airbyte /airbyte
USER airbyte

ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.1.0 \
      io.airbyte.name=airbyte/source-orca-scan
