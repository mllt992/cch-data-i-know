from dataclasses import dataclass

import asyncpg

from app.config import settings


@dataclass
class LogSchema:
    table_schema: str
    table_name: str
    timestamp_col: str
    model_col: str | None
    channel_col: str | None
    channel_name_col: str | None
    channel_id_col: str | None
    channel_lookup_schema: str | None
    channel_lookup_table: str | None
    channel_lookup_id_col: str | None
    channel_lookup_name_col: str | None
    request_key_col: str | None
    cache_creation_tokens_col: str | None
    cache_creation_5m_tokens_col: str | None
    cache_creation_1h_tokens_col: str | None
    cache_read_tokens_col: str | None
    cost_col: str | None
    prompt_tokens_col: str | None
    completion_tokens_col: str | None
    total_tokens_col: str | None
    status_col: str | None
    error_col: str | None
    latency_col: str | None


TIMESTAMP_CANDIDATES = [
    "created_at",
    "request_time",
    "called_at",
    "log_time",
    "timestamp",
    "ts",
    "time",
    "createdon",
]
MODEL_CANDIDATES = ["model", "model_name", "model_id", "llm_model", "engine"]
CHANNEL_CANDIDATES = [
    "channel_name",
    "provider_name",
    "vendor_name",
    "supplier_name",
    "supplier",
    "provider",
    "vendor",
    "channel",
    "source",
    "platform",
    "supplier_id",
    "provider_id",
    "vendor_id",
    "channel_id",
]
CHANNEL_NAME_CANDIDATES = [
    "channel_name",
    "provider_name",
    "vendor_name",
    "supplier_name",
    "platform_name",
]
CHANNEL_ID_CANDIDATES = ["channel_id", "provider_id", "vendor_id", "supplier_id"]
CHANNEL_LOOKUP_NAME_CANDIDATES = [
    "channel_name",
    "provider_name",
    "vendor_name",
    "supplier_name",
    "display_name",
    "name",
    "title",
    "label",
]
CHANNEL_LOOKUP_ID_CANDIDATES = [
    "id",
    "provider_id",
    "channel_id",
    "vendor_id",
    "supplier_id",
]
CHANNEL_LOOKUP_TABLE_TOKENS = ("provider", "channel", "vendor", "supplier")
CACHE_CREATION_TOKENS_CANDIDATES = [
    "cache_creation_input_tokens",
    "cache_creation_tokens",
    "cache_write_tokens",
    "cache_create_tokens",
]
CACHE_CREATION_5M_TOKENS_CANDIDATES = [
    "cache_creation5m_input_tokens",
    "cache_creation_5m_input_tokens",
]
CACHE_CREATION_1H_TOKENS_CANDIDATES = [
    "cache_creation1h_input_tokens",
    "cache_creation_1h_input_tokens",
]
CACHE_READ_TOKENS_CANDIDATES = [
    "cache_read_input_tokens",
    "cache_read_tokens",
    "cache_hit_tokens",
]
COST_CANDIDATES = ["cost", "total_cost", "fee", "amount", "price", "charge"]
PROMPT_TOKENS_CANDIDATES = ["prompt_tokens", "input_tokens"]
COMPLETION_TOKENS_CANDIDATES = ["completion_tokens", "output_tokens"]
TOTAL_TOKENS_CANDIDATES = ["total_tokens", "tokens", "token_count", "usage_tokens"]
STATUS_CANDIDATES = ["is_success", "status", "status_code", "code", "state", "success"]
ERROR_CANDIDATES = [
    "error",
    "error_message",
    "err",
    "exception",
    "failure_reason",
    "blocked_by",
]
LATENCY_CANDIDATES = [
    "latency_ms",
    "duration_ms",
    "elapsed_ms",
    "response_time_ms",
    "latency",
    "duration",
    "elapsed",
]
KEY_IDENTITY_CANDIDATES = [
    "api_key",
    "apikey",
    "api_token",
    "access_token",
    "auth_token",
    "authorization",
    "token",
    "key",
    "secret_key",
    "client_key",
    "client_token",
    "user_token",
]


def _pick_column(
    columns: dict[str, str],
    exact_candidates: list[str],
    fuzzy_tokens: list[str] | None = None,
) -> str | None:
    for candidate in exact_candidates:
        if candidate in columns:
            return candidate
    if not fuzzy_tokens:
        return None
    for col in columns:
        if all(token in col for token in fuzzy_tokens):
            return col
    return None


def _pick_timestamp(columns: dict[str, str]) -> str | None:
    picked = _pick_column(columns, TIMESTAMP_CANDIDATES)
    if picked:
        return picked

    for col, dtype in columns.items():
        lowered = dtype.lower()
        if "timestamp" in lowered or lowered == "date":
            return col
    return None


def _pick_channel(columns: dict[str, str]) -> str | None:
    name_picked, id_picked = _pick_channel_parts(columns)
    if name_picked:
        return name_picked
    if id_picked:
        return id_picked

    priority_tokens = ["supplier", "provider", "vendor", "channel", "source", "platform"]
    for token in priority_tokens:
        text_like = [
            col
            for col, dtype in columns.items()
            if token in col and "id" not in col and _is_text_type(dtype)
        ]
        if text_like:
            return text_like[0]

    for token in priority_tokens:
        fallback = [col for col in columns if token in col and "id" not in col]
        if fallback:
            return fallback[0]

    for token in priority_tokens:
        fallback = [col for col in columns if token in col]
        if fallback:
            return fallback[0]
    return None


def _pick_request_key(columns: dict[str, str]) -> str | None:
    for candidate in KEY_IDENTITY_CANDIDATES:
        if candidate in columns and _is_text_type(columns[candidate]):
            return candidate

    ignored_keywords = (
        "prompt",
        "completion",
        "cache",
        "total",
        "count",
        "usage",
        "input",
        "output",
    )
    for col, dtype in columns.items():
        if not _is_text_type(dtype):
            continue
        if "token" not in col and "key" not in col and "authorization" not in col:
            continue
        if any(keyword in col for keyword in ignored_keywords):
            continue
        return col
    return None


def _is_text_type(dtype: str) -> bool:
    lowered = dtype.lower()
    return any(
        token in lowered
        for token in ("char", "text", "json", "xml")
    )


def _pick_channel_parts(columns: dict[str, str]) -> tuple[str | None, str | None]:
    name_col = _pick_column(columns, CHANNEL_NAME_CANDIDATES)
    id_col = _pick_column(columns, CHANNEL_ID_CANDIDATES)

    if not name_col:
        priority_tokens = ["supplier", "provider", "vendor", "source", "platform"]
        for token in priority_tokens:
            text_cols = [
                col
                for col, dtype in columns.items()
                if token in col and "id" not in col and _is_text_type(dtype)
            ]
            if text_cols:
                name_col = text_cols[0]
                break

    if not name_col:
        name_like = [col for col in columns if "name" in col and "id" not in col]
        if name_like:
            name_col = name_like[0]

    if (
        not name_col
        and "channel" in columns
        and _is_text_type(columns["channel"])
    ):
        name_col = "channel"

    if not id_col:
        priority_tokens = ["supplier", "provider", "vendor", "channel", "source", "platform"]
        for token in priority_tokens:
            id_cols = [col for col in columns if token in col and col.endswith("_id")]
            if id_cols:
                id_col = id_cols[0]
                break

    if not id_col and "channel" in columns:
        if not _is_text_type(columns["channel"]):
            id_col = "channel"

    if not id_col:
        for fallback in ("provider", "vendor", "supplier", "channel"):
            if fallback in columns and not _is_text_type(columns[fallback]):
                id_col = fallback
                break

    return name_col, id_col


def _score_table(table_name: str, columns: dict[str, str]) -> int:
    score = 0
    has_timestamp = _pick_timestamp(columns) is not None
    if not has_timestamp:
        return -1
    score += 5

    if _pick_column(columns, MODEL_CANDIDATES, ["model"]):
        score += 2
    if _pick_column(columns, COST_CANDIDATES, ["cost"]):
        score += 2
    if _pick_column(columns, TOTAL_TOKENS_CANDIDATES, ["token"]):
        score += 2
    channel_name, channel_id = _pick_channel_parts(columns)
    if channel_name:
        score += 1
    elif channel_id:
        score += 1
    if _pick_column(columns, STATUS_CANDIDATES, ["status"]):
        score += 1
    if any(key in table_name for key in ("log", "request", "call", "usage", "invoke")):
        score += 2
    return score


async def detect_log_schema(pool: asyncpg.Pool) -> LogSchema:
    rows = await pool.fetch(
        """
        SELECT
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type
        FROM information_schema.columns c
        JOIN information_schema.tables t
            ON c.table_schema = t.table_schema
           AND c.table_name = t.table_name
        WHERE t.table_type = 'BASE TABLE'
          AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
        """
    )

    grouped: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        key = (row["table_schema"], row["table_name"])
        grouped.setdefault(key, {})
        grouped[key][row["column_name"].lower()] = row["data_type"]

    best_key: tuple[str, str] | None = None
    best_columns: dict[str, str] | None = None
    best_score = -1

    for key, columns in grouped.items():
        score = _score_table(key[1].lower(), columns)
        if score > best_score:
            best_score = score
            best_key = key
            best_columns = columns

    if best_key is None or best_columns is None or best_score < 0:
        raise RuntimeError("No suitable log table found for statistics.")

    timestamp_col = _pick_timestamp(best_columns)
    if not timestamp_col:
        raise RuntimeError("Log table detected but no timestamp column was found.")

    channel_name_col, channel_id_col = _pick_channel_parts(best_columns)
    if settings.channel_name_column_override:
        override = settings.channel_name_column_override.lower()
        if override in best_columns:
            channel_name_col = override
    if settings.channel_id_column_override:
        override = settings.channel_id_column_override.lower()
        if override in best_columns:
            channel_id_col = override

    selected_channel_col = channel_name_col or channel_id_col or _pick_channel(best_columns)
    request_key_col = _pick_request_key(best_columns)
    if settings.key_column_override:
        override = settings.key_column_override.lower()
        if override in best_columns:
            request_key_col = override
    lookup_schema = None
    lookup_table = None
    lookup_id_col = None
    lookup_name_col = None
    if not channel_name_col and channel_id_col:
        resolved = await _resolve_lookup_override(pool)
        if resolved:
            lookup_schema, lookup_table, lookup_id_col, lookup_name_col = resolved
        else:
            (
                lookup_schema,
                lookup_table,
                lookup_id_col,
                lookup_name_col,
            ) = await _detect_channel_lookup_table(
                pool=pool,
                table_schema=best_key[0],
                table_name=best_key[1],
                channel_id_col=channel_id_col,
            )
            if not lookup_table:
                (
                    lookup_schema,
                    lookup_table,
                    lookup_id_col,
                    lookup_name_col,
                ) = await _detect_channel_lookup_table_by_heuristic(
                    pool=pool,
                    table_schema=best_key[0],
                    table_name=best_key[1],
                    channel_id_col=channel_id_col,
                )

    return LogSchema(
        table_schema=best_key[0],
        table_name=best_key[1],
        timestamp_col=timestamp_col,
        model_col=_pick_column(best_columns, MODEL_CANDIDATES, ["model"]),
        channel_col=selected_channel_col,
        channel_name_col=channel_name_col,
        channel_id_col=channel_id_col,
        channel_lookup_schema=lookup_schema,
        channel_lookup_table=lookup_table,
        channel_lookup_id_col=lookup_id_col,
        channel_lookup_name_col=lookup_name_col,
        request_key_col=request_key_col,
        cache_creation_tokens_col=_pick_column(
            best_columns,
            CACHE_CREATION_TOKENS_CANDIDATES,
            ["cache", "creation", "token"],
        ),
        cache_creation_5m_tokens_col=_pick_column(
            best_columns,
            CACHE_CREATION_5M_TOKENS_CANDIDATES,
            ["cache", "5m", "token"],
        ),
        cache_creation_1h_tokens_col=_pick_column(
            best_columns,
            CACHE_CREATION_1H_TOKENS_CANDIDATES,
            ["cache", "1h", "token"],
        ),
        cache_read_tokens_col=_pick_column(
            best_columns,
            CACHE_READ_TOKENS_CANDIDATES,
            ["cache", "read", "token"],
        ),
        cost_col=_pick_column(best_columns, COST_CANDIDATES, ["cost"]),
        prompt_tokens_col=_pick_column(best_columns, PROMPT_TOKENS_CANDIDATES, ["prompt", "token"]),
        completion_tokens_col=_pick_column(
            best_columns,
            COMPLETION_TOKENS_CANDIDATES,
            ["completion", "token"],
        ),
        total_tokens_col=_pick_column(best_columns, TOTAL_TOKENS_CANDIDATES, ["token"]),
        status_col=_pick_column(best_columns, STATUS_CANDIDATES, ["status"]),
        error_col=_pick_column(best_columns, ERROR_CANDIDATES, ["error"]),
        latency_col=_pick_column(best_columns, LATENCY_CANDIDATES, ["latency"]),
    )


async def _detect_channel_lookup_table(
    pool: asyncpg.Pool,
    table_schema: str,
    table_name: str,
    channel_id_col: str,
) -> tuple[str | None, str | None, str | None, str | None]:
    fk_rows = await pool.fetch(
        """
        SELECT
            ccu.table_schema AS ref_table_schema,
            ccu.table_name AS ref_table_name,
            ccu.column_name AS ref_column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
           AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
           AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = $1
          AND tc.table_name = $2
          AND kcu.column_name = $3
        """,
        table_schema,
        table_name,
        channel_id_col,
    )

    for fk in fk_rows:
        ref_schema = fk["ref_table_schema"]
        ref_table = fk["ref_table_name"]
        ref_id_col = fk["ref_column_name"].lower()

        ref_cols_rows = await pool.fetch(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            """,
            ref_schema,
            ref_table,
        )
        ref_cols = {row["column_name"].lower(): row["data_type"] for row in ref_cols_rows}
        name_col = _pick_column(ref_cols, CHANNEL_LOOKUP_NAME_CANDIDATES)
        if not name_col:
            for col, dtype in ref_cols.items():
                if "name" in col and _is_text_type(dtype):
                    name_col = col
                    break
        if name_col:
            return ref_schema, ref_table, ref_id_col, name_col
    return None, None, None, None


def _parse_table_ref(raw: str) -> tuple[str | None, str | None]:
    text = raw.strip().strip('"')
    if not text:
        return None, None
    if "." in text:
        schema, table = text.split(".", 1)
        return schema.strip().strip('"').lower(), table.strip().strip('"').lower()
    return None, text.lower()


async def _resolve_lookup_override(
    pool: asyncpg.Pool,
) -> tuple[str, str, str, str] | None:
    if not settings.channel_lookup_table_override:
        return None

    override_schema, override_table = _parse_table_ref(settings.channel_lookup_table_override)
    if not override_table:
        return None

    rows = await pool.fetch(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
          AND table_name = $1
        """,
        override_table,
    )
    matched = None
    for row in rows:
        schema = row["table_schema"].lower()
        table = row["table_name"].lower()
        if override_schema and schema != override_schema:
            continue
        matched = (schema, table)
        break
    if not matched:
        return None

    col_rows = await pool.fetch(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        """,
        matched[0],
        matched[1],
    )
    cols = {row["column_name"].lower(): row["data_type"] for row in col_rows}

    id_col = None
    if settings.channel_lookup_id_column_override:
        candidate = settings.channel_lookup_id_column_override.lower()
        if candidate in cols:
            id_col = candidate
    if not id_col:
        id_col = _pick_column(cols, CHANNEL_LOOKUP_ID_CANDIDATES)

    name_col = None
    if settings.channel_lookup_name_column_override:
        candidate = settings.channel_lookup_name_column_override.lower()
        if candidate in cols:
            name_col = candidate
    if not name_col:
        name_col = _pick_column(cols, CHANNEL_LOOKUP_NAME_CANDIDATES)
    if not name_col:
        for col, dtype in cols.items():
            if "name" in col and _is_text_type(dtype):
                name_col = col
                break

    if id_col and name_col:
        return matched[0], matched[1], id_col, name_col
    return None


async def _detect_channel_lookup_table_by_heuristic(
    pool: asyncpg.Pool,
    table_schema: str,
    table_name: str,
    channel_id_col: str,
) -> tuple[str | None, str | None, str | None, str | None]:
    rows = await pool.fetch(
        """
        SELECT
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON c.table_schema = t.table_schema
         AND c.table_name = t.table_name
        WHERE t.table_type = 'BASE TABLE'
          AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
          AND NOT (c.table_schema = $1 AND c.table_name = $2)
        """,
        table_schema,
        table_name,
    )

    grouped: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        key = (row["table_schema"].lower(), row["table_name"].lower())
        grouped.setdefault(key, {})
        grouped[key][row["column_name"].lower()] = row["data_type"]

    channel_token = channel_id_col.replace("_id", "")
    best_score = -1
    best: tuple[str, str, str, str] | None = None

    for (schema, table), cols in grouped.items():
        table_tokens_hit = any(token in table for token in CHANNEL_LOOKUP_TABLE_TOKENS)
        id_col = _pick_column(cols, CHANNEL_LOOKUP_ID_CANDIDATES)
        name_col = _pick_column(cols, CHANNEL_LOOKUP_NAME_CANDIDATES)
        if not name_col:
            for col, dtype in cols.items():
                if "name" in col and _is_text_type(dtype):
                    name_col = col
                    break

        if not id_col or not name_col:
            continue

        score = 0
        if table_tokens_hit:
            score += 3
        if channel_token and channel_token in table:
            score += 3
        if channel_token and channel_token in id_col:
            score += 2
        if channel_token and channel_token in name_col:
            score += 1
        if table == "providers":
            score += 3

        if score > best_score:
            best_score = score
            best = (schema, table, id_col, name_col)

    if best:
        return best
    return None, None, None, None
