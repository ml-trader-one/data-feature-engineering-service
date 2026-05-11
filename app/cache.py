from collections import deque
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Tuple

import pandas as pd


@dataclass
class CandleRow:
    instrument_uid: str
    figi: str
    interval: str
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    is_complete: bool
    source: str


class CandleBuffer:
    def __init__(self, maxlen: int = 200):
        self.maxlen = maxlen
        self._buffers: Dict[Tuple[str, str], deque[CandleRow]] = {}

    def _get_buffer(self, key: tuple[str, str]) -> deque[CandleRow]:
        if key not in self._buffers:
            self._buffers[key] = deque(maxlen=self.maxlen)
        return self._buffers[key]

    def seed(self, key: tuple[str, str], candles: list[CandleRow]) -> None:
        buf = deque(candles[-self.maxlen:], maxlen=self.maxlen)
        self._buffers[key] = buf

    def upsert(self, candle: CandleRow) -> None:
        key = (candle.instrument_uid, candle.interval)
        buf = self._get_buffer(key)

        if buf and buf[-1].time == candle.time:
            buf[-1] = candle
            return

        for i in range(len(buf) - 1, -1, -1):
            if buf[i].time == candle.time:
                buf[i] = candle
                return

        buf.append(candle)

    def has_key(self, key: tuple[str, str]) -> bool:
        return key in self._buffers and len(self._buffers[key]) > 0

    def size(self, key: tuple[str, str]) -> int:
        if key not in self._buffers:
            return 0
        return len(self._buffers[key])

    def to_dataframe(self, key: tuple[str, str]) -> pd.DataFrame:
        buf = self._buffers.get(key, deque())
        data = [asdict(x) for x in buf]
        df = pd.DataFrame(data)
        if not df.empty:
            df = df.sort_values("time").reset_index(drop=True)
        return df