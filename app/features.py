import math

import numpy as np
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, MACD, SMAIndicator
from ta.volatility import AverageTrueRange, BollingerBands


def build_latest_features(df: pd.DataFrame) -> dict | None:
    if df.empty:
        return None

    df = df.copy().sort_values("time").reset_index(drop=True)

    df["return_1"] = df["close"].pct_change(1)
    df["return_5"] = df["close"].pct_change(5)
    df["return_10"] = df["close"].pct_change(10)
    df["log_return_1"] = np.log(df["close"] / df["close"].shift(1))

    df["sma_5"] = SMAIndicator(close=df["close"], window=5).sma_indicator()
    df["sma_10"] = SMAIndicator(close=df["close"], window=10).sma_indicator()
    df["sma_20"] = SMAIndicator(close=df["close"], window=20).sma_indicator()
    df["sma_50"] = SMAIndicator(close=df["close"], window=50).sma_indicator()

    df["ema_12"] = EMAIndicator(close=df["close"], window=12).ema_indicator()
    df["ema_26"] = EMAIndicator(close=df["close"], window=26).ema_indicator()

    macd = MACD(close=df["close"], window_slow=26, window_fast=12, window_sign=9)
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_diff"] = macd.macd_diff()

    bb = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb_high"] = bb.bollinger_hband()
    df["bb_low"] = bb.bollinger_lband()
    df["bb_mid"] = bb.bollinger_mavg()
    df["bb_width"] = (df["bb_high"] - df["bb_low"]) / df["bb_mid"]

    df["rsi_14"] = RSIIndicator(close=df["close"], window=14).rsi()

    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=14)
    df["atr_14"] = atr.average_true_range()

    df["volatility_20"] = df["return_1"].rolling(20).std()
    df["volume_sma_20"] = df["volume"].rolling(20).mean()
    df["volume_ratio_20"] = df["volume"] / df["volume_sma_20"]

    df["candle_body"] = df["close"] - df["open"]
    df["candle_range"] = df["high"] - df["low"]
    df["upper_shadow"] = df["high"] - df[["open", "close"]].max(axis=1)
    df["lower_shadow"] = df[["open", "close"]].min(axis=1) - df["low"]

    latest = df.iloc[-1].replace([np.inf, -np.inf], np.nan)

    if latest.isna().any():
        return None

    return {
        "instrument_uid": latest["instrument_uid"],
        "figi": latest["figi"],
        "interval": latest["interval"],
        "time": latest["time"].isoformat(),
        "open": float(latest["open"]),
        "high": float(latest["high"]),
        "low": float(latest["low"]),
        "close": float(latest["close"]),
        "volume": int(latest["volume"]),
        "source": latest["source"],
        "features": {
            "return_1": float(latest["return_1"]),
            "return_5": float(latest["return_5"]),
            "return_10": float(latest["return_10"]),
            "log_return_1": float(latest["log_return_1"]),
            "sma_5": float(latest["sma_5"]),
            "sma_10": float(latest["sma_10"]),
            "sma_20": float(latest["sma_20"]),
            "sma_50": float(latest["sma_50"]),
            "ema_12": float(latest["ema_12"]),
            "ema_26": float(latest["ema_26"]),
            "macd": float(latest["macd"]),
            "macd_signal": float(latest["macd_signal"]),
            "macd_diff": float(latest["macd_diff"]),
            "bb_high": float(latest["bb_high"]),
            "bb_low": float(latest["bb_low"]),
            "bb_mid": float(latest["bb_mid"]),
            "bb_width": float(latest["bb_width"]),
            "rsi_14": float(latest["rsi_14"]),
            "atr_14": float(latest["atr_14"]),
            "volatility_20": float(latest["volatility_20"]),
            "volume_sma_20": float(latest["volume_sma_20"]),
            "volume_ratio_20": float(latest["volume_ratio_20"]),
            "candle_body": float(latest["candle_body"]),
            "candle_range": float(latest["candle_range"]),
            "upper_shadow": float(latest["upper_shadow"]),
            "lower_shadow": float(latest["lower_shadow"]),
        },
    }