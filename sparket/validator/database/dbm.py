"""
Database manager for the validator.

Goals:
- Provide a per-process manager singleton with a larger pool (default ~50)
- Provide lightweight worker instances (default ~10) for multi-process usage
- Expose simple read/write helpers and a transactional decorator

Notes:
- Requires an async Postgres URL, e.g. postgresql+asyncpg://user:pass@host:port/db
- The Config object is expected to provide `database_url`; alternatively, the
  DATABASE_URL environment variable will be used.
- When use_null_pool=True, creates fresh connections per request to avoid
  event loop conflicts (slower but safe for cross-loop access).
"""

import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Any, Callable, Mapping, Optional, Union

from sparket.validator.config.config import Config
from sqlalchemy import text
from sqlalchemy.pool import NullPool

# Suppress verbose SQLAlchemy engine logging
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
from sqlalchemy.engine import Result
from sqlalchemy.sql.elements import TextClause, ClauseElement
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


class DBM:
    _manager_instance: Optional["DBM"] = None
    _manager_lock: threading.Lock = threading.Lock()

    def __init__(
        self,
        config: Config,
        *,
        pool_size: int = 50,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 1800,
        echo: bool = False,
        use_null_pool: bool = False,
    ) -> None:
        self.config = config

        database_url: Optional[str] = getattr(config, "database_url", None) or os.getenv(
            "DATABASE_URL"
        )
        if not database_url:
            raise ValueError(
                "Database URL not provided. Set Config.database_url or DATABASE_URL."
            )

        # NullPool creates fresh connections per request - slower but avoids
        # event loop conflicts when DB is accessed from multiple async contexts
        # (e.g., main loop + axon handlers + background tasks)
        if use_null_pool:
            self.engine: AsyncEngine = create_async_engine(
                database_url,
                echo=echo,
                poolclass=NullPool,
                future=True,
            )
        else:
            self.engine: AsyncEngine = create_async_engine(
                database_url,
                echo=echo,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle,
                pool_pre_ping=True,
                pool_reset_on_return="rollback",
                future=True,
            )

        self.session_maker: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )

    # Factories
    @classmethod
    def get_manager(
        cls,
        config: Config,
        *,
        pool_size: int = 50,
        max_overflow: int = 10,
        echo: bool = False,
        use_null_pool: bool = True,  # Default True to avoid cross-loop issues
    ) -> "DBM":
        """Get or create the singleton DBM instance.
        
        Args:
            use_null_pool: If True (default), creates fresh connections per request.
                          This is slower but avoids event loop conflicts when the
                          validator uses multiple async contexts (main loop, axon
                          handlers, background tasks). Set to False only if you're
                          certain all DB access happens in the same event loop.
        """
        if cls._manager_instance is not None:
            return cls._manager_instance
        with cls._manager_lock:
            if cls._manager_instance is None:
                cls._manager_instance = cls(
                    config,
                    pool_size=pool_size,
                    max_overflow=max_overflow,
                    echo=echo,
                    use_null_pool=use_null_pool,
                )
        return cls._manager_instance

    @classmethod
    def create_worker(
        cls,
        config: Config,
        *,
        pool_size: int = 10,
        max_overflow: int = 5,
        echo: bool = False,
        use_null_pool: bool = False,  # Workers typically run in single loop
    ) -> "DBM":
        """Create an independent DBM instance for worker processes.
        
        Workers typically run in a single event loop, so pooling is safe.
        """
        return cls(
            config,
            pool_size=pool_size,
            max_overflow=max_overflow,
            echo=echo,
            use_null_pool=use_null_pool,
        )

    # Session helpers
    @asynccontextmanager
    async def session(self) -> AsyncSession:
        async with self.session_maker() as session:
            yield session

    # CRUD helpers
    async def read(
        self,
        query: Any,
        params: Optional[Mapping[str, Any]] = None,
        *,
        mappings: bool = False,
    ) -> list[Any]:
        """
        Execute a read-only statement and return all rows.
        - If `mappings` is True, returns a list of dict-like row mappings.
        """
        if isinstance(query, str):
            raise TypeError(
                "Raw SQL strings are disallowed. Use sqlalchemy.text() with binds or Core constructs."
            )
        if not isinstance(query, (TextClause, ClauseElement)):
            raise TypeError(
                "Query must be a SQLAlchemy TextClause or ClauseElement."
            )
        stmt = query

        async with self.session() as session:
            result: Result = await session.execute(stmt, params or {})
            if mappings:
                return result.mappings().all()
            return result.all()

    async def write(
        self,
        query: Any,
        params: Optional[Mapping[str, Any]] = None,
        *,
        return_rows: bool = False,
        mappings: bool = False,
    ) -> Union[int, list[Any]]:
        """
        Execute a write statement inside a transaction.
        - If `return_rows` is True, returns rows (useful with INSERT ... RETURNING).
        - Otherwise returns the number of rows affected.
        """
        if isinstance(query, str):
            raise TypeError(
                "Raw SQL strings are disallowed. Use sqlalchemy.text() with binds or Core constructs."
            )
        if not isinstance(query, (TextClause, ClauseElement)):
            raise TypeError(
                "Query must be a SQLAlchemy TextClause or ClauseElement."
            )
        if params is None:
            raise ValueError(
                "Parameterized writes are required. Provide a params mapping."
            )
        stmt = query

        async with self.session() as session:
            async with session.begin():
                result: Result = await session.execute(stmt, params or {})
                if return_rows:
                    if mappings:
                        rows = result.mappings().all()
                    else:
                        rows = result.all()
                    return rows
                return result.rowcount or 0

    async def write_many(
        self,
        query: Any,
        params_list: list[Mapping[str, Any]],
    ) -> int:
        """
        Execute multiple write statements in a single transaction.
        Uses executemany for batch efficiency.
        Returns total rows affected.
        """
        if isinstance(query, str):
            raise TypeError(
                "Raw SQL strings are disallowed. Use sqlalchemy.text() with binds."
            )
        if not isinstance(query, (TextClause, ClauseElement)):
            raise TypeError(
                "Query must be a SQLAlchemy TextClause or ClauseElement."
            )
        if not params_list:
            return 0

        async with self.session() as session:
            async with session.begin():
                # Execute all in single transaction
                total = 0
                for params in params_list:
                    result: Result = await session.execute(query, params)
                    total += result.rowcount or 0
                return total

    # Transaction tooling
    def transactional(self) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Decorator that provides a session and wraps execution in a transaction.
        - If the wrapped coroutine raises, the transaction is rolled back.
        - On success, the transaction is committed.
        The wrapped function must be an async function and accept `session=` kwarg.
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                async with self.session() as session:
                    async with session.begin():
                        kwargs["session"] = session
                        return await func(*args, **kwargs)

            return wrapper

        return decorator

    async def dispose(self) -> None:
        await self.engine.dispose()
