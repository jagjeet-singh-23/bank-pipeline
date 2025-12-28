"""
Redis-based state manager for tracking extraction progress.
Uses continuation tokens to track S3 pagination state per table.
"""

from typing import Optional
import redis

from utils import config


def get_redis_client() -> redis.Redis:
    """
    Create and return a Redis client instance.

    Returns:
        redis.Redis: Redis client connected to the configured host/port/db.
    """
    return redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        db=config.REDIS_DB,
        decode_responses=True,  # Automatically decode bytes to strings
    )


def get_continuation_token(table: str) -> Optional[str]:
    """
    Get the continuation token for a specific table.

    Args:
        table (str): Table name to get the continuation token for.

    Returns:
        Optional[str]: The continuation token if it exists, None otherwise.
    """
    client = get_redis_client()
    key = f"extraction:continuation:{table}"
    token = client.get(key)
    return token if token else None


def set_continuation_token(table: str, token: str) -> None:
    """
    Store the continuation token for a specific table.

    Args:
        table (str): Table name to store the continuation token for.
        token (str): The continuation token to store.
    """
    client = get_redis_client()
    key = f"extraction:continuation:{table}"
    client.set(key, token)


def clear_continuation_token(table: str) -> None:
    """
    Clear the continuation token for a specific table.
    Called when all files for a table have been processed.

    Args:
        table (str): Table name to clear the continuation token for.
    """
    client = get_redis_client()
    key = f"extraction:continuation:{table}"
    client.delete(key)
