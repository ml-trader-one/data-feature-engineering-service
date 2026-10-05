import asyncio
import logging

import logging_loki
import structlog

from app.config import settings
from app.service import FeatureEngineeringService

SERVICE_LOGGER_NAME = "data_feature_engineering_service"


def configure_logging():
    root_logger = logging.getLogger()
    existing_service_handlers = [
        handler
        for handler in root_logger.handlers
        if getattr(handler, "_data_feature_service_handler", False)
    ]
    for handler in existing_service_handlers:
        root_logger.removeHandler(handler)
        handler.close()

    loki_handler = logging_loki.LokiHandler(
        url=f"{settings.loki_url}/loki/api/v1/push",
        tags={"service": SERVICE_LOGGER_NAME},
        version="1",
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer()
            if settings.log_level == "DEBUG"
            else structlog.processors.JSONRenderer(),
        ],
        foreign_pre_chain=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
        ],
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler._data_feature_service_handler = True

    loki_handler._data_feature_service_handler = True
    root_logger.addHandler(handler)
    root_logger.addHandler(loki_handler)
    root_logger.setLevel(logging.getLevelName(settings.log_level))

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level)
        ),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )


async def _main():
    configure_logging()
    service = FeatureEngineeringService()
    try:
        await service.start()
        await service.run()
    finally:
        await service.stop()


def run():
    asyncio.run(_main())


if __name__ == "__main__":
    run()
