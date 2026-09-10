# =============================================================================
# Café Cloud — automatización
# =============================================================================
#
# Aquí solo hay objetivos que ejecutan algo de verdad. `seed` se añadirá en
# TICKET-011, que es el ticket que trae los datos de ejemplo: un objetivo que no
# hace nada es peor que su ausencia.
#
# `lint` y `test` no necesitan Python en la máquina: corren dentro de la etapa
# `dev` de la imagen del servicio, que es donde viven ruff, mypy y pytest.
#
# Uso:
#   make up                 levanta todo el entorno en segundo plano
#   make build              construye las imágenes de los servicios
#   make ps                 estado y salud de los contenedores
#   make logs               sigue los logs de todos; make logs S=postgres, de uno
#   make migrate            alembic upgrade head de las DOS cadenas, cada una en
#                           un contenedor de un solo uso y con su propio rol
#   make lint               ruff + mypy sobre los cuatro proyectos y las pruebas
#                           de integración
#   make test               unitarias + integración; las de integración exigen el
#                           entorno levantado (`make up`)
#   make test-unit          solo unitarias, sin entorno y sin red: ciclo rápido
#   make test-integration   solo el flujo completo, dentro del Compose
#   make psql               shell de psql sobre la base cafecloud
#   make redis-cli          shell de redis-cli
#   make dlq-inspect STREAM=orders.created   entradas de la DLQ de ese stream
#   make dlq-replay  STREAM=orders.created   reinyecta la DLQ en su stream y la
#                           vacía; LIMIT=n acota el lote y KEEP=1 no borra nada
#   make mongosh            shell de mongosh sobre la base cafecloud
#   make down               detiene y elimina los contenedores (los datos siguen)
#   make down ARGS=-v       además borra los volúmenes
#   make clean              contenedores, volúmenes e imágenes locales fuera
# =============================================================================

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

# Servicio opcional para `logs`; vacío significa todos.
S    ?=
# Argumentos extra para `down`, típicamente -v.
ARGS ?=
# Stream de los objetivos de DLQ, y ajustes del reproceso.
STREAM ?=
LIMIT  ?= 100
KEEP   ?= 0

.PHONY: up build down clean ps logs migrate lint test test-unit test-integration \
	dev-images orders-dev-image processor-dev-image notifier-dev-image cleanup-dev-image \
	integration-image psql redis-cli mongosh dlq-inspect dlq-replay

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

# Las de integración corren dentro de la red del Compose contra los servicios vivos.
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
