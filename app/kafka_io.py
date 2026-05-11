import json
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.config import settings


class KafkaIO:
    def __init__(self):
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            settings.kafka_topic_raw_candles,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            compression_type="zstd",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self.consumer.start()
        await self.producer.start()

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

    async def publish_enriched(self, payload: dict[str, Any], key: str):
        assert self.producer is not None
        await self.producer.send_and_wait(
            settings.kafka_topic_enriched,
            key=key.encode("utf-8"),
            value=payload,
        )