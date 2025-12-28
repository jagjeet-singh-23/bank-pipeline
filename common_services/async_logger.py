import asyncio
import logging
import sys
from typing import Optional, Any


class AsyncLogger:
    """
    Asynchronous logger wrapper that uses asyncio.Queue to process log records
    in a separate background task, attempting to mimic the standard logging interface
    while preventing blocking I/O in the main event loop.
    """

    def __init__(self, name: str, level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """Starts the background logging worker."""
        if not self._running:
            self._running = True
            # We create the task. It will run in the current loop.
            self._worker_task = asyncio.create_task(self._process_queue())

    async def stop(self):
        """Stops the background logging worker and flushes the queue."""
        self._running = False
        await self.queue.put(None)  # Signal to stop
        if self._worker_task:
            await self._worker_task
            self._worker_task = None

    async def _process_queue(self):
        """Worker that consumes log records from the queue."""
        while True:
            record = await self.queue.get()
            if record is None:
                self.queue.task_done()
                break

            level, msg, args, kwargs = record
            try:
                # Dispatch to underlying logger
                if level == logging.DEBUG:
                    self.logger.debug(msg, *args, **kwargs)
                elif level == logging.INFO:
                    self.logger.info(msg, *args, **kwargs)
                elif level == logging.WARNING:
                    self.logger.warning(msg, *args, **kwargs)
                elif level == logging.ERROR:
                    self.logger.error(msg, *args, **kwargs)
                elif level == logging.CRITICAL:
                    self.logger.critical(msg, *args, **kwargs)
                elif level == logging.NOTSET:  # Fallback
                    self.logger.log(level, msg, *args, **kwargs)
                else:
                    self.logger.log(level, msg, *args, **kwargs)

            except Exception as e:
                # Fallback to stderr if logging itself fails
                sys.stderr.write(f"AsyncLogger failed to log: {msg}, error: {e}\n")
            finally:
                self.queue.task_done()

    def _enqueue(self, level: int, msg: Any, *args, **kwargs):
        # Use put_nowait to avoid blocking the caller.
        try:
            self.queue.put_nowait((level, msg, args, kwargs))
        except asyncio.QueueFull:
            sys.stderr.write(f"AsyncLogger queue full, dropping message: {msg}\n")

    def debug(self, msg: Any, *args, **kwargs):
        self._enqueue(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: Any, *args, **kwargs):
        self._enqueue(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: Any, *args, **kwargs):
        self._enqueue(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: Any, *args, **kwargs):
        self._enqueue(logging.ERROR, msg, *args, **kwargs)

    def critical(self, msg: Any, *args, **kwargs):
        self._enqueue(logging.CRITICAL, msg, *args, **kwargs)

    def exception(self, msg: Any, *args, **kwargs):
        # Capture current exception info to pass to the worker
        if kwargs.get("exc_info") is None:
            kwargs["exc_info"] = sys.exc_info()
        self._enqueue(logging.ERROR, msg, *args, **kwargs)

    def log(self, level: int, msg: Any, *args, **kwargs):
        self._enqueue(level, msg, *args, **kwargs)

    # Allow setting level on the underlying logger
    def setLevel(self, level):
        self.logger.setLevel(level)
