"""
db.py - PostgreSQL connection and cached data fetching
------------------------------------------------------
Connects to finguard_oto_april on FINGUARD RDS instance.
Data is cached in-memory for 60 seconds to avoid hammering the DB
on every Dash callback while still serving near-live data.
"""

import os
import time
import pandas as pd
from functools import lru_cache
from sqlalchemy import create_engine, text

# ─── Connection ───────────────────────────────────────────────────────────────
DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://readonly:readonly@otolmsstagedbinstance.cttxlpcdrmsq.ap-south-1.rds.amazonaws.com:5432/finguard_oto_april"
)

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            DB_URL,
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=3,
            connect_args={"connect_timeout": 10},
        )
    return _engine


# ─── Cache helpers ────────────────────────────────────────────────────────────
def _time_bucket(seconds: int = 60) -> int:
    """Round current unix time to `seconds` bucket for cache invalidation."""
    return int(time.time() / seconds)


@lru_cache(maxsize=32)
def _fetch_activities_cached(days: int, bucket: int) -> pd.DataFrame:
    """
    Inner function whose result is cached by lru_cache.
    `bucket` changes every 60s, causing automatic cache refresh.
    """
    query = f"""
        SELECT
            t.id,
            (t.created AT TIME ZONE 'Asia/Kolkata') AS created,
            t.account_id,
            t.channel,
            t.disposition,
            t.sub_disposition,
            t.flow,
            t.provider,
            t.contact_number_choice,
            t.sentiment,
            t.summary,
            t.task_status,
            t.is_priority_task,
            t.ptp,
            t.outcome
        FROM activity_taskactivity t
        WHERE t.created >= NOW() - INTERVAL '{days} days'
        ORDER BY t.created ASC
    """
    with get_engine().connect() as conn:
        df = pd.read_sql(text(query), conn)
    return df


def get_activities_df(days: int = 7) -> pd.DataFrame:
    """
    Public API: returns activities DataFrame for the last `days` days.
    Cached for 5 minutes — multiple callbacks within the same window share one DB hit.
    """
    return _fetch_activities_cached(days, _time_bucket(300))


def get_db_status() -> dict:
    """Quick health check — returns connection status and row count."""
    try:
        with get_engine().connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM activity_taskactivity")).fetchone()
        return {"ok": True, "total_rows": result[0]}
    except Exception as e:
        return {"ok": False, "error": str(e)}
