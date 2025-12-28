"""
Async uploader class for uploading records to MinIO.
"""
from typing import List, Dict, Any
import asyncio
import logging
from constants import Constants
from consumer.storage import MinIOStorage

logger = logging.getLogger(__name__)


class AsyncUploader:
    """Async uploader class for uploading records to MinIO."""
    def __init__(self, storage: MinIOStorage) -> None:
        """Initialize the async uploader."""
        self.storage: MinIOStorage = storage
        self.queue: asyncio.Queue[tuple[str, List[Dict[str, Any]]]] = asyncio.Queue(
            maxsize=Constants.UPLOAD_QUEUE_SIZE
        )
        logger.info("Async uploader initialized with queue size: %s", Constants.UPLOAD_QUEUE_SIZE)

    async def start(self) -> None:
        """Start the async uploader."""
        workers = [
            asyncio.create_task(self.worker())
            for _ in range(Constants.MAX_IN_FLIGHT_UPLOADS)
        ]
        await asyncio.gather(*workers)

    async def worker(self) -> None:
        """Worker function for uploading records to MinIO."""
        while True:
            table, records = await self.queue.get()
            logger.info("Worker picked up upload task for %s with %s records", table, len(records))
            try:
                await asyncio.to_thread(self.storage.upload_records, table, records)
                logger.info("✓ Upload completed for %s", table)
            except Exception as e:
                logger.error("✗ Upload failed for %s: %s", table, str(e))
                raise
            finally:
                self.queue.task_done()

    async def submit(self, table: str, records: List[Dict[str, Any]]) -> None:
        """Submit a batch of records for uploading."""
        queue_size = self.queue.qsize()
        logger.info("Submitting %s records for %s to upload queue (current queue size: %s)", len(records), table, queue_size)
        await self.queue.put((table, records))
