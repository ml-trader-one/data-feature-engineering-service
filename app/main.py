import asyncio
import logging

import structlog

from app.config import settings
from app.service import FeatureEngineeringService


def configure_logging():
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer()
            if settings.log_level == "DEBUG"
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


async def _main():
    configure_logging()
    service = FeatureEngineeringService()
    await service.start()
    try:
        await service.run()
    finally:
        await service.stop()


def run():
    asyncio.run(_main())


if __name__ == "__main__":
    run()