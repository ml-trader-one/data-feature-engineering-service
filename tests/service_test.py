from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import app.service as m


class DummyCounterChild:
    def __init__(self):
        self.inc = Mock()


class DummyCounter:
    def __init__(self):
        self.child = DummyCounterChild()

    def labels(self, **kwargs):
        return self.child


class DummyTimer:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class DummyHistogram:
    def time(self):
        return DummyTimer()


@pytest.fixture
def service(monkeypatch):
    monkeypatch.setattr(m, "messages_processed", DummyCounter())
    monkeypatch.setattr(m, "messages_failed", DummyCounter())
    monkeypatch.setattr(m, "processing_duration", DummyHistogram())

    svc = m.FeatureEngineeringService()
    svc.repo = SimpleNamespace(
        start=AsyncMock(),
        stop=AsyncMock(),
        get_last_candles=AsyncMock(return_value=[]),
    )
    svc.kafka = SimpleNamespace(
        start=AsyncMock(),
        stop=AsyncMock(),
        publish_enriched=AsyncMock(),
        consumer=None,
    )
    svc.buffer = SimpleNamespace(
        has_key=Mock(return_value=True),
        seed=Mock(),
        upsert=Mock(),
        size=Mock(return_value=100),
        to_dataframe=Mock(return_value="df"),
    )
    return svc


def test_remember_processed_adds_key(service):
    service._remember_processed("k1")
    assert "k1" in service._recent_keys


def test_remember_processed_clears_when_too_many(service):
    service._recent_keys = {f"k{i}" for i in range(10001)}
    service._remember_processed("new_key")
    assert service._recent_keys == set()


def test_parse_time():
    dt = m.FeatureEngineeringService._parse_time("2024-01-01T12:00:00Z")
    assert dt.isoformat() == "2024-01-01T12:00:00+00:00"


@pytest.mark.asyncio
async def test_start(service, monkeypatch):
    start_http_server_mock = Mock()
    monkeypatch.setattr(m, "start_http_server", start_http_server_mock)

    await service.start()

    service.repo.start.assert_awaited_once()
    service.kafka.start.assert_awaited_once()
    start_http_server_mock.assert_called_once_with(8000)


@pytest.mark.asyncio
async def test_stop(service):
    await service.stop()

    service.kafka.stop.assert_awaited_once()
    service.repo.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_message_skips_duplicate(service):
    payload = {
        "instrument_uid": "UID1",
        "figi": "FIGI1",
        "interval": "1m",
        "time": "2024-01-01T12:00:00",
        "open": 1,
        "high": 2,
        "low": 0.5,
        "close": 1.5,
        "volume": 10,
        "source": "stream",
    }
    dedup_key = "UID1:1m:2024-01-01T12:00:00:stream"
    service._recent_keys.add(dedup_key)

    await service.process_message(payload)

    service.buffer.upsert.assert_not_called()
    service.kafka.publish_enriched.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_message_seeds_buffer_when_key_missing(service):
    payload = {
        "instrument_uid": "UID1",
        "figi": "FIGI1",
        "interval": "1m",
        "time": "2024-01-01T12:00:00",
        "open": 1,
        "high": 2,
        "low": 0.5,
        "close": 1.5,
        "volume": 10,
        "source": "historical",
    }

    service.buffer.has_key.return_value = False
    service.repo.get_last_candles = AsyncMock(return_value=["c1", "c2"])

    await service.process_message(payload)

    service.repo.get_last_candles.assert_awaited_once_with(
        instrument_uid="UID1",
        interval="1m",
        limit=m.settings.feature_window_size,
    )
    service.buffer.seed.assert_called_once_with(("UID1", "1m"), ["c1", "c2"])
    service.buffer.upsert.assert_called_once()
    service.kafka.publish_enriched.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_message_historical_only_caches(service):
    payload = {
        "instrument_uid": "UID1",
        "figi": "FIGI1",
        "interval": "1m",
        "time": "2024-01-01T12:00:00",
        "open": 1,
        "high": 2,
        "low": 0.5,
        "close": 1.5,
        "volume": 10,
        "source": "historical",
    }

    await service.process_message(payload)

    service.buffer.upsert.assert_called_once()
    service.kafka.publish_enriched.assert_not_awaited()
    assert "UID1:1m:2024-01-01T12:00:00:historical" in service._recent_keys


@pytest.mark.asyncio
async def test_process_message_not_enough_candles(service, monkeypatch):
    payload = {
        "instrument_uid": "UID1",
        "figi": "FIGI1",
        "interval": "1m",
        "time": "2024-01-01T12:00:00",
        "open": 1,
        "high": 2,
        "low": 0.5,
        "close": 1.5,
        "volume": 10,
        "source": "stream",
    }

    service.buffer.size.return_value = 2
    monkeypatch.setattr(
        m,
        "settings",
        SimpleNamespace(
            feature_window_size=100,
            min_candles_for_features=5,
        ),
    )

    await service.process_message(payload)

    service.kafka.publish_enriched.assert_not_awaited()
    assert "UID1:1m:2024-01-01T12:00:00:stream" in service._recent_keys


@pytest.mark.asyncio
async def test_process_message_skip_when_enriched_is_none(service, monkeypatch):
    payload = {
        "instrument_uid": "UID1",
        "figi": "FIGI1",
        "interval": "1m",
        "time": "2024-01-01T12:00:00",
        "open": 1,
        "high": 2,
        "low": 0.5,
        "close": 1.5,
        "volume": 10,
        "source": "stream",
    }

    monkeypatch.setattr(m, "build_latest_features", Mock(return_value=None))

    await service.process_message(payload)

    service.buffer.to_dataframe.assert_called_once_with(("UID1", "1m"))
    service.kafka.publish_enriched.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_message_publishes_enriched(service, monkeypatch):
    payload = {
        "instrument_uid": "UID1",
        "figi": "FIGI1",
        "interval": "1m",
        "time": "2024-01-01T12:00:00",
        "open": 1,
        "high": 2,
        "low": 0.5,
        "close": 1.5,
        "volume": 10,
        "source": "stream",
    }

    enriched = {"time": "2024-01-01T12:00:00", "feature_x": 123}
    monkeypatch.setattr(m, "build_latest_features", Mock(return_value=enriched))

    await service.process_message(payload)

    service.kafka.publish_enriched.assert_awaited_once_with(
        payload=enriched,
        key="UID1",
    )
    assert "UID1:1m:2024-01-01T12:00:00:stream" in service._recent_keys


@pytest.mark.asyncio
async def test_run_processes_message_and_commits(monkeypatch, service):
    msg = SimpleNamespace(
        value={"instrument_uid": "UID1"},
        topic="raw-candles",
    )

    class DummyConsumer:
        def __init__(self, items):
            self._items = items
            self.commit = AsyncMock()

        def __aiter__(self):
            async def gen():
                for item in self._items:
                    yield item

            return gen()

    consumer = DummyConsumer([msg])
    service.kafka.consumer = consumer
    service.process_message = AsyncMock()

    await service.run()

    service.process_message.assert_awaited_once_with({"instrument_uid": "UID1"})
    consumer.commit.assert_awaited_once()
    m.messages_processed.child.inc.assert_called_once()


@pytest.mark.asyncio
async def test_run_handles_processing_error(service):
    msg = SimpleNamespace(
        value={"instrument_uid": "UID1"},
        topic="raw-candles",
    )

    class DummyConsumer:
        def __init__(self, items):
            self._items = items
            self.commit = AsyncMock()

        def __aiter__(self):
            async def gen():
                for item in self._items:
                    yield item

            return gen()

    consumer = DummyConsumer([msg])
    service.kafka.consumer = consumer

    service.process_message = AsyncMock(side_effect=Exception("boom"))

    await service.run()

    consumer.commit.assert_not_awaited()
    m.messages_failed.child.inc.assert_called_once()
