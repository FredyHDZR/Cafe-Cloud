-- =============================================================================
-- Café Cloud — esquemas y roles de Postgres (TICKET-001, implementa ADR-006)
-- =============================================================================
--
-- El entrypoint oficial de `postgres:16-alpine` ejecuta este archivo con
-- `psql -v ON_ERROR_STOP=1` UNA SOLA VEZ: cuando el directorio de datos está
-- vacío. Si el volumen `pgdata` ya existe, el script NO se vuelve a ejecutar.
-- Para reaplicarlo: `docker compose down -v && docker compose up -d`.
--
-- Ninguna credencial está escrita aquí. Las contraseñas y los nombres de rol
-- llegan por variables de entorno del contenedor (definidas en
-- `docker-compose.yml` con valores por defecto de desarrollo) y se leen con la
-- metaorden `\getenv` de psql 15+.
--
-- Reparto de propiedad (ADR-006):
--   - esquema `orders`    -> orders-service    (rol orders_rw)
--   - esquema `processor` -> processor-service (rol processor_rw)
-- =============================================================================

\getenv db_name          POSTGRES_DB
\getenv orders_role      ORDERS_DB_USER
\getenv orders_pw        ORDERS_DB_PASSWORD
\getenv processor_role   PROCESSOR_DB_USER
\getenv processor_pw     PROCESSOR_DB_PASSWORD

\echo 'Café Cloud: creando esquemas y roles sobre la base' :"db_name"

-- -----------------------------------------------------------------------------
-- 1. Roles de aplicación, uno por servicio, ambos con LOGIN.
--    Sin SUPERUSER, sin CREATEDB y sin CREATEROLE: solo lo que necesitan.
-- -----------------------------------------------------------------------------
CREATE ROLE :"orders_role"    WITH LOGIN PASSWORD :'orders_pw'    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE ROLE :"processor_role" WITH LOGIN PASSWORD :'processor_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;

-- -----------------------------------------------------------------------------
-- 2. Endurecimiento de la base y del esquema `public`.
--    Por defecto PUBLIC puede conectarse a cualquier base y, en versiones
--    anteriores a la 15, crear objetos en `public`. Se cierran las dos puertas
--    para que nadie acabe creando tablas fuera de su esquema por descuido.
-- -----------------------------------------------------------------------------
REVOKE ALL ON DATABASE :"db_name" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db_name" TO :"orders_role";
GRANT CONNECT ON DATABASE :"db_name" TO :"processor_role";

REVOKE ALL    ON SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- Se devuelve solo USAGE (nunca CREATE) para que `public` siga siendo
-- utilizable como último elemento del search_path sin permitir crear nada.
GRANT USAGE ON SCHEMA public TO :"orders_role";
GRANT USAGE ON SCHEMA public TO :"processor_role";

-- -----------------------------------------------------------------------------
-- 3. Un esquema por servicio, cada uno propiedad de su rol.
--    El dueño puede crear, alterar y borrar dentro del suyo; sobre el ajeno no
--    tiene ningún permiso salvo los que se concedan de forma explícita.
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS orders    AUTHORIZATION :"orders_role";
CREATE SCHEMA IF NOT EXISTS processor AUTHORIZATION :"processor_role";

-- -----------------------------------------------------------------------------
-- 4. Contrato estrecho de processor-service sobre el esquema ajeno (ADR-006).
--    USAGE le deja resolver `orders.orders`, pero NO leer, escribir ni crear
--    nada: cualquier CREATE TABLE en `orders` debe fallar con permiso denegado.
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA orders TO :"processor_role";

-- ¡Ojo! Los permisos finos que ADR-006 exige sobre la tabla de pedidos:
--
--     GRANT SELECT                                  ON orders.orders TO processor_rw;
--     GRANT UPDATE (status, updated_at, completed_at) ON orders.orders TO processor_rw;
--
-- NO se conceden aquí, y no es un olvido: en el arranque del contenedor la
-- tabla `orders.orders` todavía no existe, así que el GRANT fallaría y con
-- ON_ERROR_STOP=1 se caería la inicialización entera. Se conceden en la
-- migración de Alembic que crea la tabla (TICKET-002), que es el único punto
-- donde la tabla y su contrato de permisos cambian a la vez.
--
-- Esa migración debe correr con el rol DUEÑO del esquema (`orders_rw`), no con
-- el superusuario: si la ejecuta `postgres`, las tablas nacen siendo suyas y
-- `orders_rw` se queda sin permisos sobre su propio esquema, con lo que habría
-- que añadir un GRANT por cada tabla y cada secuencia. Ver TICKET-002.
--
-- Se descarta ALTER DEFAULT PRIVILEGES como alternativa: concedería sobre
-- TODAS las tablas futuras del esquema `orders` -incluidas `outbox`,
-- `order_items` e `idempotency_keys`-, que es exactamente lo que ADR-006
-- prohíbe. El límite tiene que ser tabla a tabla y columna a columna.

-- -----------------------------------------------------------------------------
-- 5. search_path por rol: cada servicio trabaja en su esquema sin cualificar,
--    y `public` queda de último recurso (solo lectura de tipos y extensiones).
--    processor-service cualifica siempre `orders.orders` de forma explícita:
--    el esquema ajeno no entra en su search_path a propósito, para que ningún
--    acceso accidental pase desapercibido.
-- -----------------------------------------------------------------------------
ALTER ROLE :"orders_role"    SET search_path = orders, public;
ALTER ROLE :"processor_role" SET search_path = processor, public;

\echo 'Café Cloud: esquemas orders/processor y roles creados correctamente.'
