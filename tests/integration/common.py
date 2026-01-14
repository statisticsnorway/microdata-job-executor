import json
import os
import shutil
from pathlib import Path

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from microdata_tools import package_dataset


def _create_key_pair(vault_dir: Path):
    if not vault_dir.exists():
        os.makedirs(vault_dir)

    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    public_key = private_key.public_key()
    microdata_public_key_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    public_key_location = vault_dir / "microdata_public_key.pem"
    with open(public_key_location, "wb") as file:
        file.write(microdata_public_key_pem)

    with open(vault_dir / "microdata_private_key.pem", "wb") as file:
        file.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )


def _render_working_dir_metadata(datastore_dir: str) -> None:
    metadata_directory = Path(f"{datastore_dir}_working")
    template_dir = Path("tests/integration/resources/templates")
    template_path = template_dir / "built_metadata_template.json"
    template = open(template_path, "r", encoding="utf-8").read()
    for filename in os.listdir(metadata_directory):
        if not filename.endswith(".json"):
            continue
        dataset_name = filename[:-12]  # Remove __DRAFT.json
        content = template.replace("DATASET_NAME", dataset_name).replace(
            "ORIGIN", "working_dir"
        )
        with open(metadata_directory / filename, "w") as f:
            f.write(content)


def _render_metadata_all(metadata_all_file: Path) -> None:
    template_dir = Path("tests/integration/resources/templates")
    template_path = template_dir / "built_metadata_template.json"
    template = open(template_path, "r", encoding="utf-8").read()
    with open(metadata_all_file, "r") as f:
        metadata_all = json.load(f)
    rendered_data_structures = []
    for dataset_name in metadata_all["dataStructures"]:
        rendered_data_structures.append(
            json.loads(
                template.replace("DATASET_NAME", dataset_name).replace(
                    "ORIGIN", "metadata_all"
                )
            )
        )
    metadata_all["dataStructures"] = rendered_data_structures
    with open(metadata_all_file, "w") as f:
        json.dump(metadata_all, f)


def _package_to_input(datastore_dir: str):
    package_dir = Path("tests/integration/resources/input_datasets")
    input_dir = Path(f"{datastore_dir}_input")
    vault_dir = Path(f"{datastore_dir}/vault")
    _create_key_pair(vault_dir)
    for dataset in os.listdir(package_dir):
        package_dataset(
            rsa_keys_dir=vault_dir,
            dataset_dir=Path(package_dir / dataset),
            output_dir=Path(input_dir),
        )


def backup_resources():
    shutil.copytree(
        "tests/integration/resources", "tests/integration/resources_backup"
    )


def recover_resources_from_backup():
    shutil.rmtree("tests/integration/resources")
    shutil.move(
        "tests/integration/resources_backup", "tests/integration/resources"
    )


def prepare_datastore(datastore_dir: str, *, package_to_input: bool = False):
    """
    Prepare a datastore directory for tests by:
    - Expanding the metadata files using the template.
    - Optionally package datasets into the input directory with a newly
      generated set of keys in the datastore's vault.
    """
    if package_to_input:
        _package_to_input(datastore_dir)
    _render_working_dir_metadata(datastore_dir)
    metadata_dir = f"{datastore_dir}/datastore"
    for filename in os.listdir(metadata_dir):
        if "metadata_all" in filename:
            _render_metadata_all(Path(f"{metadata_dir}/{filename}"))
    tmp_dir = f"{datastore_dir}/datastore/tmp"
    if not os.path.exists(tmp_dir):
        return
    for filename in os.listdir(tmp_dir):
        if "metadata_all" in filename:
            _render_metadata_all(Path(f"{tmp_dir}/{filename}"))
