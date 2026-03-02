import asyncio
import math
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Awaitable, Callable, Literal

import asyncpg

from app.config import settings
from app.schema_detector import LogSchema, detect_log_schema


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _as_numeric(expr: str) -> str:
    return (
        f"CASE WHEN {expr} IS NULL THEN NULL "
        f"WHEN {expr}::text ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN {expr}::double precision "
        "ELSE NULL END"
    )


@dataclass
class CacheEntry:
    data: Any
    expires_at: float
    lock: asyncio.Lock


@dataclass(frozen=True)
class TimeRange:
    start: datetime
    end: datetime

    @property
    def key(self) -> str:
        return f"{self.start.isoformat()}__{self.end.isoformat()}"

    @property
    def days(self) -> int:
        return max((self.end - self.start).days, 1)


class StatsService:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self._schema: LogSchema | None = None
        self._schema_expires_at = 0.0
        self._schema_lock = asyncio.Lock()
        self._cache: dict[str, CacheEntry] = {}

    def _resolve_realtime_window(self, window: str) -> tuple[str, datetime | None]:
        now = datetime.now(timezone.utc)
        w = window.strip().lower()
        if w == "today":
            start = datetime.combine(now.date(), dt_time.min, timezone.utc)
            return "today", start
        if w in {"7d", "7days", "week"}:
            return "7d", now - timedelta(days=7)
        if w in {"30d", "30days", "month"}:
            return "30d", now - timedelta(days=30)
        if w == "all":
            return "all", None
        raise ValueError("window must be one of: today, 7d, 30d, all.")

    def resolve_time_range(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> TimeRange:
        now = datetime.now(timezone.utc)
        default_days = max(settings.history_days, 1)

        if end_date is None:
            end_dt = now
        else:
            end_dt = datetime.combine(end_date + timedelta(days=1), dt_time.min, timezone.utc)

        if start_date is None:
            start_dt = end_dt - timedelta(days=default_days)
        else:
            start_dt = datetime.combine(start_date, dt_time.min, timezone.utc)

        if start_dt >= end_dt:
            raise ValueError("start_date must be earlier than end_date.")
        if (end_dt - start_dt).days > settings.max_range_days:
            raise ValueError(
                f"time range is too large, max allowed is {settings.max_range_days} days."
            )

        return TimeRange(start=start_dt, end=end_dt)

    async def _get_schema(self) -> LogSchema:
        now = time.time()
        if self._schema and now < self._schema_expires_at:
            return self._schema

        async with self._schema_lock:
            now = time.time()
            if self._schema and now < self._schema_expires_at:
                return self._schema
            self._schema = await detect_log_schema(self.pool)
            self._schema_expires_at = now + settings.schema_refresh_seconds
            return self._schema

    async def _cached(
        self,
        key: str,
        ttl_seconds: int,
        loader: Callable[[], Awaitable[Any]],
    ) -> Any:
        now = time.time()
        entry = self._cache.get(key)
        if entry is None:
            entry = CacheEntry(data=None, expires_at=0.0, lock=asyncio.Lock())
            self._cache[key] = entry

        if entry.data is not None and now < entry.expires_at:
            return entry.data

        async with entry.lock:
            now = time.time()
            if entry.data is not None and now < entry.expires_at:
                return entry.data
            try:
                entry.data = await loader()
                entry.expires_at = now + ttl_seconds
                return entry.data
            except Exception:
                if entry.data is not None:
                    return entry.data
                raise

    def _table_ref(self, schema: LogSchema) -> str:
        return f"{_quote_ident(schema.table_schema)}.{_quote_ident(schema.table_name)}"

    def _col(self, col_name: str | None) -> str:
        if not col_name:
            return "NULL"
        return _quote_ident(col_name)

    def _model_expr(self, schema: LogSchema) -> str:
        if not schema.model_col:
            return "'unknown'"
        col = self._col(schema.model_col)
        return f"COALESCE(NULLIF(BTRIM({col}::text), ''), 'unknown')"

    def _channel_expr(self, schema: LogSchema) -> str:
        effective_col = schema.channel_name_col or schema.channel_col
        if not effective_col:
            return "'unknown'"
        col = self._col(effective_col)
        base = f"COALESCE(NULLIF(BTRIM({col}::text), ''), 'unknown')"
        return (
            f"CASE "
            f"WHEN {base} ~ '^[0-9]+$' THEN 'unknown' "
            f"WHEN {base} ~* '^[0-9a-f-]{{24,}}$' THEN 'unknown' "
            f"ELSE {base} END"
        )

    def _cost_expr(self, schema: LogSchema) -> str:
        if not schema.cost_col:
            return "NULL"
        return _as_numeric(self._col(schema.cost_col))

    def _latency_expr(self, schema: LogSchema) -> str:
        if not schema.latency_col:
            return "NULL"
        return _as_numeric(self._col(schema.latency_col))

    def _cache_creation_tokens_expr(self, schema: LogSchema) -> str:
        if schema.cache_creation_tokens_col:
            return _as_numeric(self._col(schema.cache_creation_tokens_col))

        parts: list[str] = []
        if schema.cache_creation_5m_tokens_col:
            parts.append(_as_numeric(self._col(schema.cache_creation_5m_tokens_col)))
        if schema.cache_creation_1h_tokens_col:
            parts.append(_as_numeric(self._col(schema.cache_creation_1h_tokens_col)))
        if not parts:
            return "NULL"
        return " + ".join([f"COALESCE({part}, 0)" for part in parts])

    def _cache_read_tokens_expr(self, schema: LogSchema) -> str:
        if not schema.cache_read_tokens_col:
            return "NULL"
        return _as_numeric(self._col(schema.cache_read_tokens_col))

    def _total_tokens_expr(self, schema: LogSchema) -> str:
        if schema.total_tokens_col:
            return _as_numeric(self._col(schema.total_tokens_col))
        prompt = _as_numeric(self._col(schema.prompt_tokens_col)) if schema.prompt_tokens_col else "NULL"
        completion = (
            _as_numeric(self._col(schema.completion_tokens_col))
            if schema.completion_tokens_col
            else "NULL"
        )
        cache_creation = self._cache_creation_tokens_expr(schema)
        cache_read = self._cache_read_tokens_expr(schema)
        if (
            prompt == "NULL"
            and completion == "NULL"
            and cache_creation == "NULL"
            and cache_read == "NULL"
        ):
            return "NULL"
        return (
            f"COALESCE({prompt}, 0) + "
            f"COALESCE({completion}, 0) + "
            f"COALESCE({cache_creation}, 0) + "
            f"COALESCE({cache_read}, 0)"
        )

    def _prompt_tokens_expr(self, schema: LogSchema) -> str:
        if not schema.prompt_tokens_col:
            return "NULL"
        return _as_numeric(self._col(schema.prompt_tokens_col))

    def _completion_tokens_expr(self, schema: LogSchema) -> str:
        if not schema.completion_tokens_col:
            return "NULL"
        return _as_numeric(self._col(schema.completion_tokens_col))

    def _success_expr(self, schema: LogSchema) -> str:
        if schema.status_col:
            status = self._col(schema.status_col)
            return (
                f"CASE WHEN {status} IS NULL THEN NULL "
                f"WHEN LOWER({status}::text) IN ('success', 'succeeded', 'ok', 'true', 't', '1') THEN TRUE "
                f"WHEN {status}::text ~ '^[0-9]+$' AND {status}::int BETWEEN 200 AND 299 THEN TRUE "
                f"WHEN LOWER({status}::text) IN ('fail', 'failed', 'error', 'false', 'f', '0') THEN FALSE "
                f"WHEN {status}::text ~ '^[0-9]+$' THEN FALSE "
                "ELSE NULL END"
            )
        if schema.error_col:
            err = self._col(schema.error_col)
            return f"CASE WHEN {err} IS NULL OR BTRIM({err}::text) = '' THEN TRUE ELSE FALSE END"
        return "NULL"

    async def get_cost_overview(self, time_range: TimeRange) -> dict[str, Any]:
        async def loader() -> dict[str, Any]:
            schema = await self._get_schema()
            table = self._table_ref(schema)
            ts = self._col(schema.timestamp_col)
            success = self._success_expr(schema)
            sql = f"""
                WITH base AS (
                    SELECT
                        {self._cost_expr(schema)} AS cost_value,
                        {self._total_tokens_expr(schema)} AS total_tokens,
                        {self._latency_expr(schema)} AS latency_value,
                        {success} AS is_success
                    FROM {table}
                    WHERE {ts} >= $1 AND {ts} < $2
                )
                SELECT
                    COUNT(*)::bigint AS total_calls,
                    COUNT(*) FILTER (WHERE is_success IS TRUE)::bigint AS success_calls,
                    ROUND(COALESCE(SUM(cost_value), 0)::numeric, 6) AS total_cost,
                    ROUND(COALESCE(AVG(cost_value), 0)::numeric, 6) AS avg_cost_per_call,
                    COALESCE(SUM(total_tokens), 0)::bigint AS total_tokens,
                    ROUND(COALESCE(AVG(total_tokens), 0)::numeric, 2) AS avg_tokens_per_call,
                    ROUND(COALESCE(AVG(latency_value), 0)::numeric, 2) AS avg_latency_ms
                FROM base
            """
            row = await self.pool.fetchrow(sql, time_range.start, time_range.end)
            total_calls = int(row["total_calls"] or 0)
            success_calls = int(row["success_calls"] or 0)
            success_rate = (success_calls / total_calls * 100.0) if total_calls else 0.0
            return {
                "total_calls": total_calls,
                "success_calls": success_calls,
                "success_rate": round(success_rate, 2),
                "total_cost": float(row["total_cost"] or 0),
                "avg_cost_per_call": float(row["avg_cost_per_call"] or 0),
                "total_tokens": int(row["total_tokens"] or 0),
                "avg_tokens_per_call": float(row["avg_tokens_per_call"] or 0),
                "avg_latency_ms": float(row["avg_latency_ms"] or 0),
            }

        key = f"cost_overview::{time_range.key}"
        return await self._cached(key, settings.refresh_cost_seconds, loader)

    async def get_model_usage(self, time_range: TimeRange) -> list[dict[str, Any]]:
        async def loader() -> list[dict[str, Any]]:
            schema = await self._get_schema()
            table = self._table_ref(schema)
            ts = self._col(schema.timestamp_col)
            model_expr = self._model_expr(schema)
            success_expr = self._success_expr(schema)
            sql = f"""
                SELECT
                    {model_expr} AS model,
                    COUNT(*)::bigint AS calls,
                    ROUND(COALESCE(SUM({self._cost_expr(schema)}), 0)::numeric, 6) AS cost,
                    COALESCE(SUM({self._total_tokens_expr(schema)}), 0)::bigint AS tokens,
                    ROUND(
                        COALESCE(
                            100.0 * COUNT(*) FILTER (WHERE {success_expr} IS TRUE) / NULLIF(COUNT(*), 0),
                            0
                        )::numeric,
                        2
                    ) AS success_rate
                FROM {table}
                WHERE {ts} >= $1 AND {ts} < $2
                GROUP BY 1
                ORDER BY calls DESC
                LIMIT 20
            """
            rows = await self.pool.fetch(sql, time_range.start, time_range.end)
            return [
                {
                    "model": row["model"],
                    "calls": int(row["calls"] or 0),
                    "cost": float(row["cost"] or 0),
                    "tokens": int(row["tokens"] or 0),
                    "success_rate": float(row["success_rate"] or 0),
                }
                for row in rows
            ]

        key = f"model_usage::{time_range.key}"
        return await self._cached(key, settings.refresh_model_seconds, loader)

    async def get_call_trend(self, time_range: TimeRange) -> list[dict[str, Any]]:
        async def loader() -> list[dict[str, Any]]:
            schema = await self._get_schema()
            table = self._table_ref(schema)
            ts = self._col(schema.timestamp_col)
            sql = f"""
                SELECT
                    TO_CHAR(date_trunc('day', {ts}), 'YYYY-MM-DD') AS day,
                    COUNT(*)::bigint AS calls,
                    ROUND(COALESCE(SUM({self._cost_expr(schema)}), 0)::numeric, 6) AS cost,
                    COALESCE(SUM({self._total_tokens_expr(schema)}), 0)::bigint AS tokens
                FROM {table}
                WHERE {ts} >= $1 AND {ts} < $2
                GROUP BY 1
                ORDER BY 1 ASC
            """
            rows = await self.pool.fetch(sql, time_range.start, time_range.end)
            return [
                {
                    "day": row["day"],
                    "calls": int(row["calls"] or 0),
                    "cost": float(row["cost"] or 0),
                    "tokens": int(row["tokens"] or 0),
                }
                for row in rows
            ]

        key = f"call_trend::{time_range.key}"
        return await self._cached(key, settings.refresh_call_seconds, loader)

    async def get_model_availability(self, time_range: TimeRange) -> list[dict[str, Any]]:
        async def loader() -> list[dict[str, Any]]:
            schema = await self._get_schema()
            table = self._table_ref(schema)
            ts = self._col(schema.timestamp_col)
            model_expr = self._model_expr(schema)
            success_expr = self._success_expr(schema)
            sql = f"""
                SELECT
                    {model_expr} AS model,
                    COUNT(*)::bigint AS total_calls,
                    COUNT(*) FILTER (WHERE {success_expr} IS TRUE)::bigint AS success_calls,
                    COUNT(*) FILTER (WHERE {success_expr} IS FALSE)::bigint AS failed_calls,
                    ROUND(
                        COALESCE(
                            100.0 * COUNT(*) FILTER (WHERE {success_expr} IS TRUE) / NULLIF(COUNT(*), 0),
                            0
                        )::numeric,
                        2
                    ) AS availability_pct,
                    MAX({ts}) AS last_call_at
                FROM {table}
                WHERE {ts} >= $1 AND {ts} < $2
                GROUP BY 1
                ORDER BY availability_pct DESC, total_calls DESC
                LIMIT 20
            """
            rows = await self.pool.fetch(sql, time_range.start, time_range.end)
            return [
                {
                    "model": row["model"],
                    "total_calls": int(row["total_calls"] or 0),
                    "success_calls": int(row["success_calls"] or 0),
                    "failed_calls": int(row["failed_calls"] or 0),
                    "availability_pct": float(row["availability_pct"] or 0),
                    "last_call_at": row["last_call_at"].isoformat() if row["last_call_at"] else None,
                }
                for row in rows
            ]

        key = f"model_availability::{time_range.key}"
        return await self._cached(key, settings.refresh_availability_seconds, loader)

    async def get_channel_usage(self, time_range: TimeRange) -> list[dict[str, Any]]:
        async def loader() -> list[dict[str, Any]]:
            schema = await self._get_schema()
            table = self._table_ref(schema)
            ts = self._col(schema.timestamp_col)
            if (
                schema.channel_name_col is None
                and schema.channel_id_col
                and schema.channel_lookup_schema
                and schema.channel_lookup_table
                and schema.channel_lookup_id_col
                and schema.channel_lookup_name_col
            ):
                l_channel_id = self._col(schema.channel_id_col)
                lookup_table = (
                    f"{_quote_ident(schema.channel_lookup_schema)}."
                    f"{_quote_ident(schema.channel_lookup_table)}"
                )
                lookup_id = _quote_ident(schema.channel_lookup_id_col)
                lookup_name = _quote_ident(schema.channel_lookup_name_col)
                ts_col = _quote_ident(schema.timestamp_col)
                cost_expr = (
                    _as_numeric(f"l.{_quote_ident(schema.cost_col)}")
                    if schema.cost_col
                    else "NULL"
                )
                if schema.total_tokens_col:
                    total_tokens_expr = _as_numeric(f"l.{_quote_ident(schema.total_tokens_col)}")
                else:
                    prompt_expr = (
                        _as_numeric(f"l.{_quote_ident(schema.prompt_tokens_col)}")
                        if schema.prompt_tokens_col
                        else "NULL"
                    )
                    completion_expr = (
                        _as_numeric(f"l.{_quote_ident(schema.completion_tokens_col)}")
                        if schema.completion_tokens_col
                        else "NULL"
                    )
                    cache_creation_expr = (
                        _as_numeric(f"l.{_quote_ident(schema.cache_creation_tokens_col)}")
                        if schema.cache_creation_tokens_col
                        else "NULL"
                    )
                    if cache_creation_expr == "NULL":
                        cache_creation_5m_expr = (
                            _as_numeric(f"l.{_quote_ident(schema.cache_creation_5m_tokens_col)}")
                            if schema.cache_creation_5m_tokens_col
                            else "NULL"
                        )
                        cache_creation_1h_expr = (
                            _as_numeric(f"l.{_quote_ident(schema.cache_creation_1h_tokens_col)}")
                            if schema.cache_creation_1h_tokens_col
                            else "NULL"
                        )
                        if cache_creation_5m_expr == "NULL" and cache_creation_1h_expr == "NULL":
                            cache_creation_expr = "NULL"
                        else:
                            cache_creation_expr = (
                                f"COALESCE({cache_creation_5m_expr}, 0) + "
                                f"COALESCE({cache_creation_1h_expr}, 0)"
                            )
                    cache_read_expr = (
                        _as_numeric(f"l.{_quote_ident(schema.cache_read_tokens_col)}")
                        if schema.cache_read_tokens_col
                        else "NULL"
                    )
                    if (
                        prompt_expr == "NULL"
                        and completion_expr == "NULL"
                        and cache_creation_expr == "NULL"
                        and cache_read_expr == "NULL"
                    ):
                        total_tokens_expr = "NULL"
                    else:
                        total_tokens_expr = (
                            f"COALESCE({prompt_expr}, 0) + "
                            f"COALESCE({completion_expr}, 0) + "
                            f"COALESCE({cache_creation_expr}, 0) + "
                            f"COALESCE({cache_read_expr}, 0)"
                        )
                channel_expr = (
                    f"CASE "
                    f"WHEN c.{lookup_name} IS NULL OR BTRIM(c.{lookup_name}::text) = '' THEN 'unknown' "
                    f"WHEN BTRIM(c.{lookup_name}::text) ~ '^[0-9]+$' THEN 'unknown' "
                    f"WHEN BTRIM(c.{lookup_name}::text) ~* '^[0-9a-f-]{{24,}}$' THEN 'unknown' "
                    f"ELSE BTRIM(c.{lookup_name}::text) END"
                )
                sql = f"""
                    SELECT
                        {channel_expr} AS channel,
                        COUNT(*)::bigint AS calls,
                        ROUND(COALESCE(SUM({cost_expr}), 0)::numeric, 6) AS cost,
                        COALESCE(SUM({total_tokens_expr}), 0)::bigint AS tokens
                    FROM {table} l
                    LEFT JOIN {lookup_table} c
                      ON l.{l_channel_id}::text = c.{lookup_id}::text
                    WHERE l.{ts_col} >= $1 AND l.{ts_col} < $2
                    GROUP BY 1
                    ORDER BY calls DESC
                    LIMIT 20
                """
            else:
                channel_expr = self._channel_expr(schema)
                sql = f"""
                    SELECT
                        {channel_expr} AS channel,
                        COUNT(*)::bigint AS calls,
                        ROUND(COALESCE(SUM({self._cost_expr(schema)}), 0)::numeric, 6) AS cost,
                        COALESCE(SUM({self._total_tokens_expr(schema)}), 0)::bigint AS tokens
                    FROM {table}
                    WHERE {ts} >= $1 AND {ts} < $2
                    GROUP BY 1
                    ORDER BY calls DESC
                    LIMIT 20
                """
            rows = await self.pool.fetch(sql, time_range.start, time_range.end)
            return [
                {
                    "channel": row["channel"],
                    "calls": int(row["calls"] or 0),
                    "cost": float(row["cost"] or 0),
                    "tokens": int(row["tokens"] or 0),
                }
                for row in rows
            ]

        key = f"channel_usage::{time_range.key}"
        return await self._cached(key, settings.refresh_channel_seconds, loader)

    async def get_realtime_availability(
        self,
        window: Literal["today", "7d", "30d", "all"] | str = "7d",
    ) -> dict[str, Any]:
        normalized_window, start_time = self._resolve_realtime_window(window)

        async def loader() -> dict[str, Any]:
            schema = await self._get_schema()
            table = self._table_ref(schema)
            ts = self._col(schema.timestamp_col)
            model_expr = self._model_expr(schema)
            success_expr = self._success_expr(schema)
            model_limit = max(settings.realtime_availability_model_limit, 1)
            event_limit = max(settings.realtime_availability_event_limit, 1)
            all_max_days = max(settings.realtime_availability_all_max_days, 1)
            effective_start = start_time
            now = datetime.now(timezone.utc)
            slot_count = event_limit
            slot_seconds = 0

            if normalized_window == "all":
                capped_start = datetime.combine(
                    (now - timedelta(days=all_max_days - 1)).date(),
                    dt_time.min,
                    timezone.utc,
                )
                min_sql = f"SELECT MIN({ts}) AS min_called_at FROM {table}"
                min_row = await self.pool.fetchrow(min_sql)
                min_called_at = min_row["min_called_at"] if min_row else None
                if min_called_at is None:
                    return {
                        "window": normalized_window,
                        "event_limit": event_limit,
                        "slot_count": all_max_days,
                        "slot_seconds": 86400,
                        "start_at": capped_start.isoformat(),
                        "end_at": now.isoformat(),
                        "models": [],
                    }
                if min_called_at.tzinfo is None:
                    min_called_at = min_called_at.replace(tzinfo=timezone.utc)
                min_data_day_start = datetime.combine(
                    min_called_at.date(),
                    dt_time.min,
                    timezone.utc,
                )
                effective_start = max(capped_start, min_data_day_start)
                slot_seconds = 86400
                slot_count = max((now.date() - effective_start.date()).days + 1, 1)
            else:
                if effective_start is None:
                    effective_start = datetime.combine(now.date(), dt_time.min, timezone.utc)
                range_seconds = max(int((now - effective_start).total_seconds()), 1)
                slot_seconds = max(math.ceil(range_seconds / slot_count), 1)

            sql = f"""
                WITH base AS (
                    SELECT
                        {model_expr} AS model,
                        {ts} AS called_at,
                        CASE
                            WHEN {success_expr} IS TRUE THEN 'success'
                            WHEN {success_expr} IS FALSE THEN 'failed'
                            ELSE 'other'
                        END AS status
                    FROM {table}
                    WHERE {ts} >= $1
                ),
                top_models AS (
                    SELECT
                        model,
                        COUNT(*)::bigint AS total_calls
                    FROM base
                    GROUP BY model
                    ORDER BY total_calls DESC, model ASC
                    LIMIT $2
                ),
                bucket_events AS (
                    SELECT
                        b.model,
                        LEAST(
                            $3 - 1,
                            GREATEST(
                                0,
                                FLOOR(EXTRACT(EPOCH FROM (b.called_at - $1)) / $4)::int
                            )
                        ) AS slot_index,
                        b.status
                    FROM base b
                    JOIN top_models tm ON tm.model = b.model
                ),
                bucketed AS (
                    SELECT
                        model,
                        slot_index,
                        SUM((status = 'success')::int)::int AS success_calls,
                        SUM((status = 'failed')::int)::int AS failed_calls,
                        SUM((status = 'other')::int)::int AS other_calls,
                        COUNT(*)::int AS slot_total_calls
                    FROM bucket_events
                    GROUP BY model, slot_index
                ),
                series AS (
                    SELECT
                        tm.model,
                        tm.total_calls AS model_total_calls,
                        s.slot_index,
                        COALESCE(bk.success_calls, 0)::int AS success_calls,
                        COALESCE(bk.failed_calls, 0)::int AS failed_calls,
                        COALESCE(bk.other_calls, 0)::int AS other_calls,
                        COALESCE(bk.slot_total_calls, 0)::int AS slot_total_calls
                    FROM top_models tm
                    CROSS JOIN generate_series(0, $3 - 1) AS s(slot_index)
                    LEFT JOIN bucketed bk
                      ON bk.model = tm.model
                     AND bk.slot_index = s.slot_index
                )
                SELECT
                    model,
                    MAX(model_total_calls)::bigint AS total_calls,
                    ARRAY_AGG(success_calls ORDER BY slot_index ASC) AS success_series,
                    ARRAY_AGG(failed_calls ORDER BY slot_index ASC) AS failed_series,
                    ARRAY_AGG(other_calls ORDER BY slot_index ASC) AS other_series,
                    ARRAY_AGG(slot_total_calls ORDER BY slot_index ASC) AS total_series
                FROM series
                GROUP BY model
                ORDER BY MAX(model_total_calls) DESC, model ASC
            """
            rows = await self.pool.fetch(
                sql,
                effective_start,
                model_limit,
                slot_count,
                slot_seconds,
            )

            models = []
            global_max_slot_calls = 0
            for row in rows:
                success_series = list(row["success_series"] or [])
                failed_series = list(row["failed_series"] or [])
                other_series = list(row["other_series"] or [])
                total_series = list(row["total_series"] or [])
                slot_len = max(
                    len(success_series),
                    len(failed_series),
                    len(other_series),
                    len(total_series),
                )
                if not slot_len:
                    continue

                slots: list[dict[str, int]] = []
                statuses: list[str] = []
                model_max_slot_calls = 0
                for i in range(slot_len):
                    success_calls = int(success_series[i] if i < len(success_series) else 0)
                    failed_calls = int(failed_series[i] if i < len(failed_series) else 0)
                    other_calls = int(other_series[i] if i < len(other_series) else 0)
                    total_calls = int(total_series[i] if i < len(total_series) else 0)
                    if total_calls <= 0:
                        total_calls = success_calls + failed_calls + other_calls

                    model_max_slot_calls = max(model_max_slot_calls, total_calls)
                    slots.append(
                        {
                            "success_calls": success_calls,
                            "failed_calls": failed_calls,
                            "other_calls": other_calls,
                            "total_calls": total_calls,
                        }
                    )
                    if total_calls <= 0:
                        statuses.append("empty")
                    elif failed_calls > 0:
                        statuses.append("failed")
                    elif success_calls > 0:
                        statuses.append("success")
                    elif other_calls > 0:
                        statuses.append("other")
                    else:
                        statuses.append("empty")

                global_max_slot_calls = max(global_max_slot_calls, model_max_slot_calls)
                models.append(
                    {
                        "model": row["model"],
                        "total_calls": int(row["total_calls"] or 0),
                        "statuses": statuses,
                        "slots": slots,
                        "max_slot_calls": model_max_slot_calls,
                    }
                )

            return {
                "window": normalized_window,
                "event_limit": event_limit,
                "slot_count": slot_count,
                "slot_seconds": slot_seconds,
                "start_at": effective_start.isoformat(),
                "end_at": now.isoformat(),
                "global_max_slot_calls": global_max_slot_calls,
                "models": models,
            }

        key = f"realtime_availability::{normalized_window}"
        return await self._cached(
            key,
            settings.refresh_realtime_availability_seconds,
            loader,
        )

    async def get_token_usage(self, time_range: TimeRange) -> dict[str, Any]:
        async def loader() -> dict[str, Any]:
            schema = await self._get_schema()
            table = self._table_ref(schema)
            ts = self._col(schema.timestamp_col)
            prompt_expr = self._prompt_tokens_expr(schema)
            completion_expr = self._completion_tokens_expr(schema)
            cache_creation_expr = self._cache_creation_tokens_expr(schema)
            cache_read_expr = self._cache_read_tokens_expr(schema)
            total_expr = self._total_tokens_expr(schema)
            model_expr = self._model_expr(schema)

            summary_sql = f"""
                SELECT
                    COALESCE(SUM({prompt_expr}), 0)::bigint AS prompt_tokens,
                    COALESCE(SUM({completion_expr}), 0)::bigint AS completion_tokens,
                    COALESCE(SUM({cache_creation_expr}), 0)::bigint AS cache_creation_tokens,
                    COALESCE(SUM({cache_read_expr}), 0)::bigint AS cache_read_tokens,
                    COALESCE(SUM({total_expr}), 0)::bigint AS total_tokens,
                    ROUND(COALESCE(AVG({total_expr}), 0)::numeric, 2) AS avg_tokens_per_call
                FROM {table}
                WHERE {ts} >= $1 AND {ts} < $2
            """
            by_model_sql = f"""
                SELECT
                    {model_expr} AS model,
                    COALESCE(SUM({prompt_expr}), 0)::bigint AS prompt_tokens,
                    COALESCE(SUM({completion_expr}), 0)::bigint AS completion_tokens,
                    COALESCE(SUM({cache_creation_expr}), 0)::bigint AS cache_creation_tokens,
                    COALESCE(SUM({cache_read_expr}), 0)::bigint AS cache_read_tokens,
                    COALESCE(SUM({total_expr}), 0)::bigint AS tokens
                FROM {table}
                WHERE {ts} >= $1 AND {ts} < $2
                GROUP BY 1
                ORDER BY tokens DESC
                LIMIT 20
            """
            trend_sql = f"""
                SELECT
                    TO_CHAR(date_trunc('day', {ts}), 'YYYY-MM-DD') AS day,
                    COALESCE(SUM({prompt_expr}), 0)::bigint AS prompt_tokens,
                    COALESCE(SUM({completion_expr}), 0)::bigint AS completion_tokens,
                    COALESCE(SUM({cache_creation_expr}), 0)::bigint AS cache_creation_tokens,
                    COALESCE(SUM({cache_read_expr}), 0)::bigint AS cache_read_tokens,
                    COALESCE(SUM({total_expr}), 0)::bigint AS tokens
                FROM {table}
                WHERE {ts} >= $1 AND {ts} < $2
                GROUP BY 1
                ORDER BY 1 ASC
            """
            summary_row = await self.pool.fetchrow(summary_sql, time_range.start, time_range.end)
            by_model_rows = await self.pool.fetch(by_model_sql, time_range.start, time_range.end)
            trend_rows = await self.pool.fetch(trend_sql, time_range.start, time_range.end)

            return {
                "summary": {
                    "prompt_tokens": int(summary_row["prompt_tokens"] or 0),
                    "completion_tokens": int(summary_row["completion_tokens"] or 0),
                    "cache_creation_tokens": int(summary_row["cache_creation_tokens"] or 0),
                    "cache_read_tokens": int(summary_row["cache_read_tokens"] or 0),
                    "cache_tokens": int(summary_row["cache_creation_tokens"] or 0)
                    + int(summary_row["cache_read_tokens"] or 0),
                    "total_tokens": int(summary_row["total_tokens"] or 0),
                    "avg_tokens_per_call": float(summary_row["avg_tokens_per_call"] or 0),
                },
                "by_model": [
                    {
                        "model": row["model"],
                        "prompt_tokens": int(row["prompt_tokens"] or 0),
                        "completion_tokens": int(row["completion_tokens"] or 0),
                        "cache_creation_tokens": int(row["cache_creation_tokens"] or 0),
                        "cache_read_tokens": int(row["cache_read_tokens"] or 0),
                        "cache_tokens": int(row["cache_creation_tokens"] or 0)
                        + int(row["cache_read_tokens"] or 0),
                        "tokens": int(row["tokens"] or 0),
                    }
                    for row in by_model_rows
                ],
                "trend": [
                    {
                        "day": row["day"],
                        "prompt_tokens": int(row["prompt_tokens"] or 0),
                        "completion_tokens": int(row["completion_tokens"] or 0),
                        "cache_creation_tokens": int(row["cache_creation_tokens"] or 0),
                        "cache_read_tokens": int(row["cache_read_tokens"] or 0),
                        "cache_tokens": int(row["cache_creation_tokens"] or 0)
                        + int(row["cache_read_tokens"] or 0),
                        "tokens": int(row["tokens"] or 0),
                    }
                    for row in trend_rows
                ],
            }

        key = f"token_usage::{time_range.key}"
        return await self._cached(key, settings.refresh_token_seconds, loader)

    async def get_dashboard(self, time_range: TimeRange) -> dict[str, Any]:
        schema = await self._get_schema()
        cost, model, trend, availability, channel, token = await asyncio.gather(
            self.get_cost_overview(time_range),
            self.get_model_usage(time_range),
            self.get_call_trend(time_range),
            self.get_model_availability(time_range),
            self.get_channel_usage(time_range),
            self.get_token_usage(time_range),
        )
        return {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "window_days": time_range.days,
            "time_range": {
                "start": time_range.start.date().isoformat(),
                "end": (time_range.end - timedelta(days=1)).date().isoformat(),
            },
            "source": {
                "table": f"{schema.table_schema}.{schema.table_name}",
                "timestamp_col": schema.timestamp_col,
                "channel_col": schema.channel_col,
                "channel_name_col": schema.channel_name_col,
                "channel_id_col": schema.channel_id_col,
                "channel_lookup_table": (
                    f"{schema.channel_lookup_schema}.{schema.channel_lookup_table}"
                    if schema.channel_lookup_schema and schema.channel_lookup_table
                    else None
                ),
                "channel_lookup_name_col": schema.channel_lookup_name_col,
                "cache_creation_tokens_col": schema.cache_creation_tokens_col,
                "cache_creation_5m_tokens_col": schema.cache_creation_5m_tokens_col,
                "cache_creation_1h_tokens_col": schema.cache_creation_1h_tokens_col,
                "cache_read_tokens_col": schema.cache_read_tokens_col,
            },
            "cost_overview": cost,
            "model_usage": model,
            "call_trend": trend,
            "model_availability": availability,
            "channel_usage": channel,
            "token_usage": token,
        }
