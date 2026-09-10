#!/usr/bin/env bash
#
# Reproceso manual de una dead letter queue (ADR-004). Lee las entradas de
# <stream>.dlq, reinyecta el envelope integro en <stream> y borra la entrada de
# la DLQ. Las DLQ no tienen consumidor automatico a proposito: esto lo dispara
# una persona despues de mirar el motivo con XRANGE.
#
#   make dlq-replay STREAM=orders.created
#   make dlq-replay STREAM=orders.completed LIMIT=10 KEEP=1
#
# LIMIT  entradas como maximo por ejecucion (por defecto 100).
# KEEP=1 deja la entrada en la DLQ en lugar de borrarla; reinyectar dos veces es
#        inofensivo porque el consumidor deduplica por event_id, pero entonces la
#        longitud de la DLQ deja de ser el trabajo pendiente.
#
# Salida: 0 todo reinyectado, 1 error de Redis, 2 uso incorrecto, 3 lote
# reinyectado con entradas omitidas por envelope ausente o ilegible.
#
set -euo pipefail

STREAM="${STREAM:-${1:-}}"
if [ -z "$STREAM" ]; then
  echo "uso: STREAM=orders.created $0 [LIMIT] ; o make dlq-replay STREAM=orders.created" >&2
  exit 2
fi

DLQ="${DLQ:-${STREAM}.dlq}"
LIMIT="${LIMIT:-100}"
KEEP="${KEEP:-0}"
COMPOSE="${COMPOSE:-docker compose}"

LUA=$(cat <<'LUA'
local entries = redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', tonumber(ARGV[1]))
local keep = ARGV[2] == '1'
local report = {}
for _, entry in ipairs(entries) do
  local envelope
  for i = 1, #entry[2], 2 do
    if entry[2][i] == 'envelope' then envelope = entry[2][i + 1] end
  end
  local command = {'XADD', KEYS[2], '*'}
  if envelope then
    local ok, fields = pcall(cjson.decode, envelope)
    if ok and type(fields) == 'table' then
      for name, value in pairs(fields) do
        command[#command + 1] = name
        command[#command + 1] = tostring(value)
      end
    end
  end
  if #command > 3 then
    local new_id = redis.call(unpack(command))
    if not keep then redis.call('XDEL', KEYS[1], entry[1]) end
    report[#report + 1] = entry[1] .. ' -> ' .. new_id
  else
    -- Una entrada ilegible no puede abortar el lote: bloquearia el reproceso de las de detras.
    report[#report + 1] = entry[1] .. ' -> OMITIDA: envelope ausente, ilegible o vacio'
  end
end
return report
LUA
)

echo "reinyectando hasta ${LIMIT} entradas de ${DLQ} en ${STREAM} (KEEP=${KEEP})"
OUTPUT=$($COMPOSE exec -T redis redis-cli EVAL "$LUA" 2 "$DLQ" "$STREAM" "$LIMIT" "$KEEP" 2>&1)
printf '%s\n' "$OUTPUT"

# redis-cli sale con 0 aunque el servidor devuelva un error, asi que el estado se mira aqui.
if printf '%s' "$OUTPUT" | grep -qE '^(ERR|\(error\))'; then
  exit 1
fi
if printf '%s' "$OUTPUT" | grep -q 'OMITIDA'; then
  echo "hay entradas omitidas: siguen en ${DLQ} y necesitan revision manual" >&2
  exit 3
fi
