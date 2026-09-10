-- Implementa ADR-006. El entrypoint de `postgres:16-alpine` solo ejecuta este archivo cuando el
-- directorio de datos esta vacio: para reaplicarlo hace falta `docker compose down -v`.

\getenv db_name          POSTGRES_DB
\getenv orders_role      ORDERS_DB_USER
\getenv orders_pw        ORDERS_DB_PASSWORD
\getenv processor_role   PROCESSOR_DB_USER
\getenv processor_pw     PROCESSOR_DB_PASSWORD

\echo 'Café Cloud: creando esquemas y roles sobre la base' :"db_name"

CREATE ROLE :"orders_role"    WITH LOGIN PASSWORD :'orders_pw'    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE ROLE :"processor_role" WITH LOGIN PASSWORD :'processor_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;

REVOKE ALL ON DATABASE :"db_name" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db_name" TO :"orders_role";
GRANT CONNECT ON DATABASE :"db_name" TO :"processor_role";

REVOKE ALL    ON SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- USAGE sin CREATE: `public` sigue sirviendo de ultimo elemento del search_path sin dejar crear.
GRANT USAGE ON SCHEMA public TO :"orders_role";
GRANT USAGE ON SCHEMA public TO :"processor_role";

CREATE SCHEMA IF NOT EXISTS orders    AUTHORIZATION :"orders_role";
CREATE SCHEMA IF NOT EXISTS processor AUTHORIZATION :"processor_role";

GRANT USAGE ON SCHEMA orders TO :"processor_role";

-- Los permisos de columna de ADR-006 sobre `orders.orders` los concede la migracion de Alembic:
-- aqui la tabla aun no existe y el GRANT caeria la inicializacion entera con ON_ERROR_STOP=1.

-- El esquema ajeno queda fuera del search_path del procesador para que ningun acceso a
-- `orders.orders` pueda ocurrir sin cualificar.
ALTER ROLE :"orders_role"    SET search_path = orders, public;
ALTER ROLE :"processor_role" SET search_path = processor, public;

\echo 'Café Cloud: esquemas orders/processor y roles creados correctamente.'
