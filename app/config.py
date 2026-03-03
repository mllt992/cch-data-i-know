import hashlib
import re
from dataclasses import dataclass

from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass(frozen=True)
class ConfiguredKey:
    name: str
    values: tuple[str, ...]
    slug: str

    @property
    def value(self) -> str:
        return self.values[0] if self.values else ""


def _slugify(value: str) -> str:
    lowered = value.strip().lower()
    slug = re.sub(r"[^\w]+", "-", lowered, flags=re.UNICODE).strip("-_")
    slug = slug.replace("_", "-")
    if slug:
        return slug
    digest = hashlib.sha1(lowered.encode("utf-8")).hexdigest()[:8]
    return f"key-{digest}"


def _parse_configured_keys(raw: str) -> list[ConfiguredKey]:
    if not raw.strip():
        return []

    normalized = raw.replace("；", ";")
    chunks = [item.strip() for item in re.split(r"[;\n]", normalized) if item.strip()]
    keys: list[ConfiguredKey] = []
    used_slugs: dict[str, int] = {}

    for chunk in chunks:
        if "|" in chunk:
            name_part, key_part = chunk.split("|", 1)
        elif "：" in chunk:
            name_part, key_part = chunk.split("：", 1)
        elif ":" in chunk:
            name_part, key_part = chunk.split(":", 1)
        else:
            continue

        name = name_part.strip()
        values = _parse_key_values(key_part)
        if not name or not values:
            continue

        base_slug = _slugify(name)
        index = used_slugs.get(base_slug, 0) + 1
        used_slugs[base_slug] = index
        slug = base_slug if index == 1 else f"{base_slug}-{index}"
        keys.append(ConfiguredKey(name=name, values=values, slug=slug))

    return keys


def _parse_key_values(raw: str) -> tuple[str, ...]:
    text = raw.strip()
    if not text:
        return tuple()

    wrapped_pairs = {
        ("(", ")"),
        ("（", "）"),
        ("[", "]"),
        ("【", "】"),
    }
    if len(text) >= 2 and (text[0], text[-1]) in wrapped_pairs:
        text = text[1:-1].strip()

    normalized = text.replace("，", ",")
    parts = [item.strip() for item in normalized.split(",")]
    clean = [
        item.strip().strip('"').strip("'")
        for item in parts
        if item.strip().strip('"').strip("'")
    ]
    if not clean:
        return tuple()
    return tuple(dict.fromkeys(clean))


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "CCH Stats Dashboard"
    database_dsn: str
    history_days: int = 30
    max_range_days: int = 180

    db_pool_min_size: int = 1
    db_pool_max_size: int = 8
    schema_refresh_seconds: int = 1800

    refresh_cost_seconds: int = 900
    refresh_model_seconds: int = 900
    refresh_call_seconds: int = 900
    refresh_availability_seconds: int = 900
    refresh_realtime_availability_seconds: int = 900
    refresh_channel_seconds: int = 900
    refresh_token_seconds: int = 900
    refresh_api_enabled: bool = False
    refresh_api_auth_key: str = ""

    channel_name_column_override: str | None = None
    channel_id_column_override: str | None = None
    channel_lookup_table_override: str | None = None
    channel_lookup_id_column_override: str | None = None
    channel_lookup_name_column_override: str | None = None
    key_column_override: str | None = None

    realtime_availability_model_limit: int = 30
    realtime_availability_event_limit: int = 120
    realtime_availability_all_max_days: int = 120
    key_records_limit: int = 100
    key_records_default_page_size: int = 10
    key_records_max_page_size: int = 100
    key_visual_refresh_seconds: int = 30
    key_visual_auto_refresh_enabled: bool = False
    key_configs: str = ""

    cors_origins: str = "*"

    @property
    def cors_origin_list(self) -> list[str]:
        raw = self.cors_origins.strip()
        if not raw or raw == "*":
            return ["*"]
        return [item.strip() for item in raw.split(",") if item.strip()]

    @property
    def configured_keys(self) -> list[ConfiguredKey]:
        return _parse_configured_keys(self.key_configs)


settings = Settings()
