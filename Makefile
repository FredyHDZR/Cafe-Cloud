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
#   make lint               ruff + mypy sobre los tres servicios
#   make test               pytest sobre los tres servicios
#   make psql               shell de psql sobre la base cafecloud
#   make redis-cli          shell de redis-cli
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

# Servicio opcional para `logs`; vacío significa todos.
S    ?=
# Argumentos extra para `down`, típicamente -v.
ARGS ?=

.PHONY: up build down clean ps logs migrate lint test dev-images orders-dev-image \
	processor-dev-image notifier-dev-image psql redis-cli mongosh

up:
	$(COMPOSE) up -d --build

build:
	$(COMPOSE) build

down:
	$(COMPOSE) down --remove-orphans $(ARGS)

clean:
	$(COMPOSE) down --remove-orphans --volumes --rmi local
	-$(DOCKER) image rm $(ORDERS_DEV_IMAGE) $(PROCESSOR_DEV_IMAGE) $(NOTIFIER_DEV_IMAGE)

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

dev-images: orders-dev-image processor-dev-image notifier-dev-image

lint: dev-images
	$(DOCKER) run --rm $(ORDERS_DEV_IMAGE) sh -c "ruff check . && ruff format --check . && mypy"
	$(DOCKER) run --rm $(PROCESSOR_DEV_IMAGE) sh -c "ruff check . && ruff format --check . && mypy"
	$(DOCKER) run --rm $(NOTIFIER_DEV_IMAGE) sh -c "ruff check . && ruff format --check . && mypy"

test: dev-images
	$(DOCKER) run --rm $(ORDERS_DEV_IMAGE) pytest
	$(DOCKER) run --rm $(PROCESSOR_DEV_IMAGE) pytest
	$(DOCKER) run --rm $(NOTIFIER_DEV_IMAGE) pytest

psql:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

redis-cli:
	$(COMPOSE) exec redis redis-cli

mongosh:
	$(COMPOSE) exec mongo mongosh $(MONGO_DB)
