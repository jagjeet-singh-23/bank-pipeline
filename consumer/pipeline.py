"""
Data Ingestion Pipeline
"""
from typing import Dict, Any
import asyncio
import logging

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from constants import Constants

from consumer.ingestor import KafkaIngestor
from consumer.uploads import AsyncUploader
from consumer.storage import MinIOStorage
from consumer.retry import DLQProducer
from consumer.batching import BatchBuffer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataIngestionPipeline:
    """Data Ingestion Pipeline class."""

    def __init__(self) -> None:
        """Initialize the Data Ingestion pipeline."""
        self.topics: list[str] = [
            "banking_server.public.customers",
            "banking_server.public.accounts",
            "banking_server.public.transactions",
        ]
        logger.info(
            "Initializing Data Ingestion Pipeline for topics: %s", self.topics
        )
        logger.info("Batch size: %s", Constants.BATCH_SIZE)

        self.ingestor = KafkaIngestor(self.topics)
        self.storage = MinIOStorage()
        self.uploader = AsyncUploader(self.storage)
        self.dlq = DLQProducer()
        self.buffer = BatchBuffer(self.topics, Constants.BATCH_SIZE)

        logger.info("Data Ingestion Pipeline initialized successfully")

    async def run(self) -> None:
        """Run the Data Ingestion pipeline."""

        logger.info("Starting Data Ingestion Pipeline...")
        asyncio.create_task(self.uploader.start())

        logger.info(
            "Started %s async upload workers", Constants.MAX_IN_FLIGHT_UPLOADS
        )

        logger.info("Waiting for Kafka messages...")
        message_count = 0
        for topic, event in self.ingestor.stream():
            message_count += 1
            logger.debug(
                "[Message #%s] Received event from topic: %s",
                message_count,
                topic,
            )
            try:
                payload: Dict[str, Any] = event.get("payload", {})
                record: Dict[str, Any] | None = payload.get("after")

                if not record:
                    logger.debug(
                        "Skipping event with no 'after' payload from %s", topic
                    )
                    continue

                if self.buffer.add(topic, record):
                    table: str = topic.split(".")[-1]
                    records = self.buffer.flush(topic)
                    logger.info(
                        "✓ Batch ready for %s: %s records. "
                        "Submitting for upload...",
                        table,
                        len(records),
                    )
                    await self.uploader.submit(table, records)

            except Exception as e:
                logger.error(
                    "✗ Error processing message from %s: %s", topic, e
                )
                self.dlq.send(topic, event, e)


if __name__ == "__main__":
    pipeline = DataIngestionPipeline()
    asyncio.run(pipeline.run())
