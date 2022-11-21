from pathlib import Path


def get_file_list_from_dir(directory: Path):
    """
    Returns a list of files found in the specified folder
    """
    return [content for content in directory.iterdir() if content.is_file()]
