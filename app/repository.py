from datetime import datetime

import asyncpg

from app.cache import CandleRow
from app.config import settings


class CandleRepository:
    def __init__(self):
        self.pool: asyncpg.Pool | None = None

    async def start(self):
        self.pool = await asyncpg.create_pool(settings.postgres_dsn, min_size=1, max_size=5)

    async def stop(self):
        if self.pool:
            await self.pool.close()

    async def get_last_candles(
        self,
        instrument_uid: str,
        interval: str,
        limit: int = 200,
    ) -> list[CandleRow]:
        query = """
        SELECT instrument_uid, figi, interval, time, open, high, low, close, volume, is_complete, source
        FROM candles
        WHERE instrument_uid = $1 AND interval = $2
        ORDER BY time DESC
        LIMIT $3
        """
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, instrument_uid, interval, limit)

        rows = list(reversed(rows))
        return [
            CandleRow(
                instrument_uid=r["instrument_uid"],
                figi=r["figi"],
                interval=r["interval"],
                time=r["time"],
                open=float(r["open"]),
                high=float(r["high"]),
                low=float(r["low"]),
                close=float(r["close"]),
                volume=int(r["volume"]),
                is_complete=bool(r["is_complete"]),
                source=r["source"],
            )
            for r in rows
        ]