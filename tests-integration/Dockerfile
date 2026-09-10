# syntax=docker/dockerfile:1.7

FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    POETRY_VERSION=1.8.5 \
    POETRY_NO_INTERACTION=1

RUN pip install "poetry==${POETRY_VERSION}"

WORKDIR /build

COPY pyproject.toml poetry.lock ./

# El lock manda: --no-update falla si pyproject y poetry.lock se han desincronizado.
RUN poetry check --lock \
    && poetry export --only main --without-hashes -f requirements.txt -o requirements.txt \
    && poetry export --with dev --without-hashes -f requirements.txt -o requirements-dev.txt

RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt


# Este proyecto no tiene etapa `runtime`: no despliega nada, solo ejecuta pruebas.
FROM builder AS dev

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# redis-cli es lo que ejecuta scripts/dlq-replay.sh a traves del sustituto de bin/.
RUN apt-get update \
    && apt-get install -y --no-install-recommends redis-tools \
    && rm -rf /var/lib/apt/lists/*

RUN /opt/venv/bin/pip install --no-cache-dir -r requirements-dev.txt

WORKDIR /app
COPY . .

CMD ["pytest"]
