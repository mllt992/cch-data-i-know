from pydantic_settings import BaseSettings, SettingsConfigDict


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
    schema_refresh_seconds: int = 600

    refresh_cost_seconds: int = 300
    refresh_model_seconds: int = 300
    refresh_call_seconds: int = 300
    refresh_availability_seconds: int = 300
    refresh_realtime_availability_seconds: int = 300
    refresh_channel_seconds: int = 300
    refresh_token_seconds: int = 300

    channel_name_column_override: str | None = None
    channel_id_column_override: str | None = None
    channel_lookup_table_override: str | None = None
    channel_lookup_id_column_override: str | None = None
    channel_lookup_name_column_override: str | None = None

    realtime_availability_model_limit: int = 30
    realtime_availability_event_limit: int = 120
    realtime_availability_all_max_days: int = 120

    cors_origins: str = "*"

    @property
    def cors_origin_list(self) -> list[str]:
        raw = self.cors_origins.strip()
        if not raw or raw == "*":
            return ["*"]
        return [item.strip() for item in raw.split(",") if item.strip()]


settings = Settings()
