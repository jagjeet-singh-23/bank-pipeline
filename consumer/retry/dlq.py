from typing import Any, Dict
import json
from kafka import KafkaProducer
from datetime import datetime
from constants import Constants


class DLQProducer:
    def __init__(self) -> None:
        self.producer: KafkaProducer = KafkaProducer(
            bootstrap_servers=Constants.KAFKA_BOOTSTRAP,
            value_serializer=lambda x: json.dumps(x).encode(),
        )

    def send(self, topic: str, message: Dict[str, Any], error: Exception) -> None:
        payload: Dict[str, Any] = {
            "source_topic": topic,
            "message": message,
            "error": str(error),
            "timestamp": datetime.utcnow().isoformat(),
        }
        self.producer.send(Constants.DLQ_TOPIC, payload)
