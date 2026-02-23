"""Standalone SDIO ingestor process.

Runs the SportsDataIO data-ingestion pipeline as an independent PM2-managed
process, fully decoupled from the validator.  All state is shared through
the database — no in-memory coupling.

Usage:
    pm2 start ecosystem.config.js --only ingestor-local
    python sparket/entrypoints/ingestor.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path


def _clear_bytecode_cache() -> int:
    base = Path(__file__).parent.parent
    removed = 0
    for cache_dir in base.rglob("__pycache__"):
        try:
            import shutil
            shutil.rmtree(cache_dir)
            removed += 1
        except (OSError, PermissionError):
            pass
    return removed


_CLEARED = _clear_bytecode_cache()
if _CLEARED > 0:
    print(f"[ingestor] Cleared {_CLEARED} bytecode cache directories", file=sys.stderr, flush=True)

import bittensor as bt
from dotenv import load_dotenv


async def run_ingest_loop(
    database,
    ingestor,
    *,
    interval_seconds: int = 60,
) -> None:
    """Perpetual ingest loop with exponential backoff on errors."""
    from sparket.validator.handlers.ingest.provider_ingest import upsert_provider_closing
    from sparket.providers.providers import get_provider_id

    MIN_BACKOFF = 30.0
    MAX_BACKOFF = 600.0
    FACTOR = 2.0

    bt.logging.info({
        "ingestor_loop": {
            "interval_seconds": interval_seconds,
            "status": "starting",
        }
    })

    consecutive_errors = 0
    error_backoff = MIN_BACKOFF
    running = True

    def _stop(sig, _frame):
        nonlocal running
        bt.logging.info({"ingestor_loop": "signal_received", "signal": sig})
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    while running:
        cycle_start = datetime.now(timezone.utc)
        try:
            await ingestor.run_once(now=cycle_start)

            provider_id = get_provider_id("SDIO")
            if provider_id:
                try:
                    upserts = await upsert_provider_closing(
                        database=database,
                        provider_id=provider_id,
                        close_ts=cycle_start,
                    )
                    if upserts:
                        bt.logging.debug({"ingestor_closing_upserts": upserts})
                except Exception as exc:
                    bt.logging.warning({"ingestor_closing_error": str(exc)})

            consecutive_errors = 0
            error_backoff = MIN_BACKOFF

        except asyncio.CancelledError:
            break
        except Exception as e:
            consecutive_errors += 1
            bt.logging.error({
                "ingestor_error": {
                    "error": str(e),
                    "type": type(e).__name__,
                    "consecutive": consecutive_errors,
                    "backoff_seconds": error_backoff,
                }
            })
            try:
                await asyncio.sleep(error_backoff)
            except asyncio.CancelledError:
                break
            error_backoff = min(error_backoff * FACTOR, MAX_BACKOFF)
            continue

        elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
        sleep_time = max(1, interval_seconds - elapsed)

        bt.logging.info({
            "ingestor_cycle": {
                "elapsed_seconds": round(elapsed, 1),
                "next_in_seconds": round(sleep_time, 1),
            }
        })

        try:
            await asyncio.sleep(sleep_time)
        except asyncio.CancelledError:
            break

    bt.logging.info({"ingestor_loop": "stopped"})
    await ingestor.close()


def _init_database():
    """Synchronous DB init — must run OUTSIDE asyncio.run()."""
    from sparket.validator.config.config import Config as ValidatorAppConfig
    from sparket.validator.database.init import initialize as init_db
    from sparket.validator.database.dbm import DBM
    from sparket.config.db_url import ensure_config_database_url

    app_config = ValidatorAppConfig()

    try:
        core_db = getattr(getattr(app_config, "core", None), "database", None)
        ensure_config_database_url(core_db)
    except Exception as e:
        bt.logging.warning({"ingestor_db_url_error": str(e)})

    init_db(app_config)
    database = DBM.get_manager(app_config, use_null_pool=False)
    bt.logging.info({"ingestor_init": "database_ready"})
    return app_config, database


async def main(app_config, database) -> None:
    from sparket.validator.services import SportsDataIngestor

    ingestor = SportsDataIngestor(database=database)
    restored = await ingestor.initialize_from_db()
    bt.logging.info({"ingestor_init": "ready", "events_restored": restored})

    interval = 60
    try:
        timers = getattr(getattr(app_config, "core", None), "timers", None)
        if timers is not None:
            interval = max(30, int(getattr(timers, "sdio_ingest_interval_seconds", 60)))
    except Exception:
        pass

    await run_ingest_loop(database, ingestor, interval_seconds=interval)
    await database.dispose()


if __name__ == "__main__":
    from sparket.shared.logging import suppress_bittensor_header_warnings

    suppress_bittensor_header_warnings()
    bt.logging.setLevel("TRACE")
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)

    bt.logging.info("Starting SDIO ingestor")

    os.chdir(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    bt.logging.info({"cwd": os.getcwd()})

    test_mode = os.getenv("SPARKET_TEST_MODE", "").lower() in ("true", "1", "yes")
    if not test_mode:
        load_dotenv(".env", verbose=True, override=True)

    try:
        from sparket.config.db_url import ensure_env_database_url
        ensure_env_database_url()
    except Exception as e:
        bt.logging.warning({"db_url_composition_error": str(e)})

    # DB init is synchronous (uses run_until_complete internally),
    # so it must happen BEFORE asyncio.run().
    _app_config, _database = _init_database()
    asyncio.run(main(_app_config, _database))
