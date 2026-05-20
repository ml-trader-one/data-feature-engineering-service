import asyncio
import logging

import structlog

from app.cache import CandleBuffer, CandleRow
from app.config import settings
from app.features import build_latest_features
from app.kafka_io import KafkaIO
from app.repository import CandleRepository

from prometheus_client import start_http_server, Counter, Histogram

logger = structlog.get_logger(__name__)


messages_processed = Counter(
    'kafka_messages_processed_total',
    'Total processed messages',
    ['topic']
)

processing_duration = Histogram(
    'message_processing_duration_seconds',
    'Time spent processing a message'
)

messages_failed = Counter(
    'kafka_messages_failed_total',
    'Total failed messages',
    ['topic']
)


class FeatureEngineeringService:
    def __init__(self):
        self.repo = CandleRepository()
        self.kafka = KafkaIO()
        self.buffer = CandleBuffer(maxlen=settings.feature_window_size)
        self._recent_keys: set[str] = set()

    async def start(self):
        await self.repo.start()
        await self.kafka.start()
        start_http_server(8000)
        logger.info("Feature service started")

    async def stop(self):
        await self.kafka.stop()
        await self.repo.stop()
        logger.info("Feature service stopped")

    async def run(self):
        assert self.kafka.consumer is not None

        async for msg in self.kafka.consumer:
            try:
                with processing_duration.time():
                    await self.process_message(msg.value)
                await self.kafka.consumer.commit()
                messages_processed.labels(topic=msg.topic).inc()
            except Exception as exc:
                messages_failed.labels(topic=msg.topic).inc()
                logger.exception("Failed to process message", error=str(exc))

    async def process_message(self, payload: dict):
        dedup_key = (
            f"{payload['instrument_uid']}:"
            f"{payload['interval']}:"
            f"{payload['time']}:"
            f"{payload.get('source', 'unknown')}"
        )
        if dedup_key in self._recent_keys:
            logger.debug("Duplicate skipped", dedup_key=dedup_key)
            return
        self._recent_keys.add(dedup_key)
        if len(self._recent_keys) > 10000:
            self._recent_keys.clear()

        candle = CandleRow(
            instrument_uid=payload["instrument_uid"],
            figi=payload["figi"],
            interval=payload["interval"],
            time=self._parse_time(payload["time"]),
            open=float(payload["open"]),
            high=float(payload["high"]),
            low=float(payload["low"]),
            close=float(payload["close"]),
            volume=int(payload["volume"]),
            is_complete=bool(payload.get("is_complete", True)),
            source=payload.get("source", "unknown"),
        )

        key = (candle.instrument_uid, candle.interval)

        if not self.buffer.has_key(key):
            candles = await self.repo.get_last_candles(
                instrument_uid=candle.instrument_uid,
                interval=candle.interval,
                limit=settings.feature_window_size,
            )
            if candles:
                self.buffer.seed(key, candles)

        self.buffer.upsert(candle)

        if candle.source != "stream":
            logger.debug("Historical candle cached only", instrument_uid=candle.instrument_uid)
            return

        if self.buffer.size(key) < settings.min_candles_for_features:
            logger.info(
                "Not enough candles for features",
                instrument_uid=candle.instrument_uid,
                interval=candle.interval,
                size=self.buffer.size(key),
            )
            return

        df = self.buffer.to_dataframe(key)
        enriched = build_latest_features(df)
        if enriched is None:
            logger.debug("Latest row contains NaN, skip publish", instrument_uid=candle.instrument_uid)
            return

        await self.kafka.publish_enriched(
            payload=enriched,
            key=candle.instrument_uid,
        )
        logger.info(
            "Enriched candle published",
            instrument_uid=candle.instrument_uid,
            interval=candle.interval,
            time=enriched["time"],
        )

    @staticmethod
    def _parse_time(value: str):
        from datetime import datetime
        return datetime.fromisoformat(value.replace("Z", "+00:00"))