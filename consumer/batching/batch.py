"""
Batch buffer class for buffering records before uploading to MinIO.
"""
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


class BatchBuffer:
    """Batch buffer class."""
    def __init__(self, topics: List[str], batch_size: int) -> None:
        """Initialize the batch buffer."""
        self.batch_size: int = batch_size
        self.buffer: Dict[str, List[Dict[str, Any]]] = {topic: [] for topic in topics}
        logger.info("Batch buffer initialized with size: %s", batch_size)

    def _is_batch_full(self, topic: str) -> bool:
        """Check if the batch buffer is full."""
        return len(self.buffer[topic]) >= self.batch_size

    def add(self, topic: str, record: Dict[str, Any]) -> bool:
        """Add a record to the batch buffer."""
        self.buffer[topic].append(record)
        is_full = self._is_batch_full(topic)
        logger.debug(
            "Buffer for %s: %s/%s records%s (FULL - ready to flush)",
            topic,
            len(self.buffer[topic]),
            self.batch_size,
            "" if is_full else "",
        )
        return is_full

    def flush(self, topic: str) -> List[Dict[str, Any]]:
        """Flush the batch buffer."""
        records = self.buffer[topic]
        logger.debug("Flushing %s records from %s buffer", len(records), topic)
        self.buffer[topic] = []
        return records
