# Si existe .env, sus valores mandan sobre los de abajo (por eso van con ?=).
-include .env

DOCKER        ?= docker
COMPOSE       ?= $(DOCKER) compose
POSTGRES_USER ?= postgres
POSTGRES_DB   ?= cafecloud
MONGO_DB      ?= cafecloud

ORDERS_DIR          ?= orders-service
ORDERS_DEV_IMAGE    ?= cafecloud/orders-service:dev
PROCESSOR_DIR       ?= processor-service
PROCESSOR_DEV_IMAGE ?= cafecloud/processor-service:dev
NOTIFIER_DIR        ?= notifier-service
NOTIFIER_DEV_IMAGE  ?= cafecloud/notifier-service:dev
CLEANUP_DIR         ?= cleanup-job
CLEANUP_DEV_IMAGE   ?= cafecloud/cleanup-job:dev
INTEGRATION_IMAGE   ?= cafecloud/integration-tests:dev
INTEGRATION_SERVICE ?= integration-tests
SEED_SERVICE        ?= seed

S      ?=
ARGS   ?=
STREAM ?=
LIMIT  ?= 100
KEEP   ?= 0

.DEFAULT_GOAL := help

.PHONY: help up build down clean ps logs migrate seed lint test test-unit test-integration \
	dev-images orders-dev-image processor-dev-image notifier-dev-image cleanup-dev-image \
	integration-image psql redis-cli mongosh dlq-inspect dlq-replay

help:
	@echo 'Café Cloud'
	@echo ''
	@echo '  make up               levanta todo el entorno en segundo plano, construyendo lo que falte'
	@echo '  make build            construye las imagenes sin arrancar nada'
	@echo '  make ps               estado y salud de los contenedores'
	@echo '  make logs             sigue los logs de todos; S=postgres sigue los de uno'
	@echo '  make migrate          alembic upgrade head de las dos cadenas, cada una con su rol'
	@echo '  make seed             crea pedidos de ejemplo por HTTP y espera sus notificaciones'
	@echo '  make test             unitarias e integracion; las de integracion exigen el entorno'
	@echo '  make test-unit        solo unitarias, sin entorno y sin red'
	@echo '  make test-integration solo el flujo completo, dentro de la red del Compose'
	@echo '  make lint             ruff y mypy sobre los cinco proyectos'
	@echo '  make psql             shell de psql sobre la base cafecloud'
	@echo '  make redis-cli        shell de redis-cli'
	@echo '  make mongosh          shell de mongosh sobre la base cafecloud'
	@echo '  make dlq-inspect STREAM=orders.created   entradas de esa dead letter queue'
	@echo '  make dlq-replay  STREAM=orders.created   la reinyecta y la vacia; LIMIT=n, KEEP=1'
	@echo '  make down             detiene y elimina los contenedores; ARGS=-v borra los datos'
	@echo '  make clean            contenedores, volumenes e imagenes locales fuera'

up:
	$(COMPOSE) up -d --build

build:
	$(COMPOSE) build

down:
	$(COMPOSE) down --remove-orphans $(ARGS)

clean:
	$(COMPOSE) down --remove-orphans --volumes --rmi local
	-$(DOCKER) image rm $(ORDERS_DEV_IMAGE) $(PROCESSOR_DEV_IMAGE) $(NOTIFIER_DEV_IMAGE) $(CLEANUP_DEV_IMAGE) $(INTEGRATION_IMAGE)

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100 $(S)

migrate:
	$(COMPOSE) run --rm orders-migrate
	$(COMPOSE) run --rm processor-migrate

# `run` arrastra el depends_on, asi que levanta el entorno si esta caido.
seed:
	$(COMPOSE) run --rm $(SEED_SERVICE)

orders-dev-image:
	$(DOCKER) build --target dev -t $(ORDERS_DEV_IMAGE) $(ORDERS_DIR)

processor-dev-image:
	$(DOCKER) build --target dev -t $(PROCESSOR_DEV_IMAGE) $(PROCESSOR_DIR)

notifier-dev-image:
	$(DOCKER) build --target dev -t $(NOTIFIER_DEV_IMAGE) $(NOTIFIER_DIR)

cleanup-dev-image:
	$(DOCKER) build --target dev -t $(CLEANUP_DEV_IMAGE) $(CLEANUP_DIR)

integration-image:
	$(COMPOSE) build $(INTEGRATION_SERVICE)

dev-images: orders-dev-image processor-dev-image notifier-dev-image cleanup-dev-image

# ruff, ruff format y mypy viven en la etapa `dev` de cada imagen: no hace falta Python en la maquina.
lint: dev-images integration-image
	$(DOCKER) run --rm $(ORDERS_DEV_IMAGE) sh -c "ruff check . && ruff format --check . && mypy"
	$(DOCKER) run --rm $(PROCESSOR_DEV_IMAGE) sh -c "ruff check . && ruff format --check . && mypy"
	$(DOCKER) run --rm $(NOTIFIER_DEV_IMAGE) sh -c "ruff check . && ruff format --check . && mypy"
	$(DOCKER) run --rm $(CLEANUP_DEV_IMAGE) sh -c "ruff check . && ruff format --check . && mypy"
	$(DOCKER) run --rm $(INTEGRATION_IMAGE) sh -c "ruff check . && ruff format --check . && mypy"

test: test-unit test-integration

test-unit: dev-images
	$(DOCKER) run --rm $(ORDERS_DEV_IMAGE) pytest
	$(DOCKER) run --rm $(PROCESSOR_DEV_IMAGE) pytest
	$(DOCKER) run --rm $(NOTIFIER_DEV_IMAGE) pytest
	$(DOCKER) run --rm $(CLEANUP_DEV_IMAGE) pytest

# Corren dentro de la red del Compose, contra los servicios vivos.
test-integration: integration-image
	$(COMPOSE) run --rm $(INTEGRATION_SERVICE)

psql:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

redis-cli:
	$(COMPOSE) exec redis redis-cli

mongosh:
	$(COMPOSE) exec mongo mongosh $(MONGO_DB)

dlq-inspect:
	$(COMPOSE) exec -T redis redis-cli XRANGE $(STREAM).dlq - +

dlq-replay:
	COMPOSE="$(COMPOSE)" STREAM="$(STREAM)" LIMIT="$(LIMIT)" KEEP="$(KEEP)" ./scripts/dlq-replay.sh
