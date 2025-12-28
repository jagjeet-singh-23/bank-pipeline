from abc import ABC, abstractmethod
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
    def download_objects(self, prefix: str) -> List[str]:
        """Method to download objects from storage.
        Args:
            prefix (str): Prefix to filter objects.
        Returns:
            List[str]: List of objects.
        """
        raise NotImplementedError
