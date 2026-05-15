from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    postgres_host: str = Field("localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(5432, alias="POSTGRES_PORT")
    postgres_db: str = Field("market_data", alias="POSTGRES_DB")
    postgres_user: str = Field("postgres", alias="POSTGRES_USER")
    postgres_password: str = Field("postgres", alias="POSTGRES_PASSWORD")

    kafka_bootstrap_servers: str = Field("localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_raw_candles: str = Field("raw_candles", alias="KAFKA_TOPIC_RAW_CANDLES")
    kafka_topic_enriched: str = Field("enriched_features", alias="KAFKA_TOPIC_ENRICHED")
    kafka_group_id: str = Field("feature-service", alias="KAFKA_GROUP_ID")

    feature_window_size: int = Field(200, alias="FEATURE_WINDOW_SIZE")
    min_candles_for_features: int = Field(50, alias="MIN_CANDLES_FOR_FEATURES")

    log_level: str = Field("INFO", alias="LOG_LEVEL")

    loki_url: str = Field("localhost:3100", alias="LOKI_URL")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()