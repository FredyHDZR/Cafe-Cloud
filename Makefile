# =============================================================================
# Café Cloud — automatización
# =============================================================================
#
# Aquí solo hay objetivos que ejecutan algo de verdad. `migrate`, `seed`, `test`
# y `lint` se añaden en el ticket que trae lo que ejecutan (TICKET-002 y
# siguientes): un objetivo que no hace nada es peor que su ausencia.
#
# Uso:
#   make up                 levanta los tres almacenes en segundo plano
#   make ps                 estado y salud de los contenedores
#   make logs               sigue los logs de todos; make logs S=postgres, de uno
#   make psql               shell de psql sobre la base cafecloud
#   make redis-cli          shell de redis-cli
#   make mongosh            shell de mongosh sobre la base cafecloud
#   make down               detiene y elimina los contenedores (los datos siguen)
#   make down ARGS=-v       además borra los volúmenes: estado limpio de verdad
# =============================================================================

# Si existe .env, sus valores mandan sobre los de abajo (por eso van con ?=).
-include .env

COMPOSE       ?= docker compose
POSTGRES_USER ?= postgres
POSTGRES_DB   ?= cafecloud
MONGO_DB      ?= cafecloud

# Servicio opcional para `logs`; vacío significa todos.
S    ?=
# Argumentos extra para `down`, típicamente -v.
ARGS ?=

.PHONY: up down ps logs psql redis-cli mongosh

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down --remove-orphans $(ARGS)

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100 $(S)

psql:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

redis-cli:
	$(COMPOSE) exec redis redis-cli

mongosh:
	$(COMPOSE) exec mongo mongosh $(MONGO_DB)
