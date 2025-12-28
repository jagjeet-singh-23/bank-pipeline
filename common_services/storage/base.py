from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List


class Storage(ABC):
    """
    Abstract base class for storage services.
    """

    @abstractmethod
    def upload_records(self, table: str, records: List[Dict[str, Any]]) -> None:
        """Method to upload records to storage.
        Args:
            table (str): Table name.
            records (List[Dict[str, Any]]): List of records to upload.
        """
        raise NotImplementedError

    @abstractmethod
    def download_object(self, bucket: str, key: str, local_path: Path) -> None:
        """Method to download objects from storage.
        Args:
            bucket (str): Bucket name.
            key (str): Object key.
            local_path (Path): Local path to save the object."""
        raise NotImplementedError
