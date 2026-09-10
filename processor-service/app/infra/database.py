from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.infra.config import Settings


class Database:
    def __init__(self, dsn: str, *, server_settings: Mapping[str, str], echo: bool = False) -> None:
        self._engine: AsyncEngine = create_async_engine(
            dsn,
            echo=echo,
            pool_pre_ping=True,
            connect_args={"server_settings": dict(server_settings)},
        )
        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=self._engine,
            expire_on_commit=False,
            autoflush=False,
        )

    @classmethod
    def from_settings(cls, settings: Settings) -> "Database":
        return cls(settings.sqlalchemy_dsn, server_settings=session_guards(settings))

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self._session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise

    async def ping(self) -> None:
        # No es una consulta de negocio: es el apreton de manos del driver, y por eso no vive
        # en la capa de repositorios.
        async with self._engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def dispose(self) -> None:
        await self._engine.dispose()


def session_guards(settings: Settings) -> Mapping[str, str]:
    return {
        "lock_timeout": str(settings.lock_timeout_ms),
        "idle_in_transaction_session_timeout": str(settings.idle_in_transaction_timeout_ms),
        # ADR-006: "cero filas es exito" solo se cumple bajo READ COMMITTED.
        "default_transaction_isolation": "read committed",
    }
