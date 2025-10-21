from pathlib import Path

from microdata_tools import unpackage_dataset


def unpackage(
    dataset_name: str,
    input_directory_path: Path,
    working_directory_path: Path,
    rsa_keys_directory: Path,
) -> None:
    file_path = Path(input_directory_path / "archive" / f"{dataset_name}.tar")
    unpackage_dataset(
        packaged_file_path=file_path,
        rsa_keys_dir=rsa_keys_directory,
        output_dir=working_directory_path,
    )
