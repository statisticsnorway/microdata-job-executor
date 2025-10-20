import os
import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass
class InputDirectory:
    path: Path

    def archive_importable(self, dataset_name: str) -> None:
        """
        Archives the input .tar files if not already archived
        """
        archive_dir = self.path / "archive"
        tar_filename = f"{dataset_name}.tar"
        archived_tar_file = archive_dir / tar_filename
        tar_file = self.path / tar_filename
        if not archive_dir.exists():
            os.makedirs(archive_dir, exist_ok=True)
        if tar_file.exists() and not os.path.isfile(archived_tar_file):
            shutil.move(str(tar_file), str(archive_dir))

    def delete_archived_importable(self, dataset_name: str) -> None:
        """
        Delete the archived .tar file from the archive directory.
        """
        archived_file: Path = self.path / f"archive/{dataset_name}.tar"
        if archived_file.is_file():
            os.remove(archived_file)

    def get_importable_tar_size_in_bytes(self, dataset_name: str) -> int:
        """
        Checks the size in bytes of the dataset.tar file.
        Returns size in bytes or 0 if the file does not exist.
        """
        tar_path = self.path / f"{dataset_name}.tar"

        if not tar_path.exists():
            tar_path = self.path / "archive" / f"{dataset_name}.tar"

        if tar_path.exists():
            return os.path.getsize(tar_path)
        return 0
