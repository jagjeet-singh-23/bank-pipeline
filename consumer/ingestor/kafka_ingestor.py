"""
Kafka Ingestor
"""
from typing import Generator, Tuple, Dict, Any
import json
import logging
from kafka import KafkaConsumer
from constants import Constants

logger = logging.getLogger(__name__)


class KafkaIngestor:
    """Kafka Ingestor class."""
    def __init__(self, topics: list[str]) -> None:
        """Initialize the Kafka Ingestor."""

        logger.info("Connecting to Kafka at %s", Constants.KAFKA_BOOTSTRAP)
        logger.info("Subscribing to topics: %s", topics)
        logger.info("Consumer group: %s", Constants.KAFKA_GROUP)

        self.consumer: KafkaConsumer = KafkaConsumer(
            *topics,
            bootstrap_servers=Constants.KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=Constants.KAFKA_GROUP,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        logger.info("Kafka consumer connected successfully")

    def stream(self) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
        """Stream messages from Kafka."""
        logger.info("Starting to consume messages from Kafka...")
        for message in self.consumer:
            logger.debug(
                "Consumed message from %s [partition=%s, offset=%s]",
                message.topic,
                message.partition,
                message.offset,
            )
            yield message.topic, message.value
