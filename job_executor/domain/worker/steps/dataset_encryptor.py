import logging
from pathlib import Path

from pyarrow import dataset, parquet
from pyarrow.parquet.encryption import (
    EncryptionConfiguration,
    KmsConnectionConfig,
)

from job_executor.adapter.kms_client import (
    COLUMN_KEY_ID,
    FOOTER_KEY_ID,
    make_crypto_factory,
)
from job_executor.common.exceptions import BuilderStepError

logger = logging.getLogger()


def dataset_encryption_config(
    column_names: list[str],
) -> dataset.ParquetEncryptionConfig:
    encryption_configuration = EncryptionConfiguration(
        footer_key=FOOTER_KEY_ID,
        column_keys={COLUMN_KEY_ID: column_names},
        encryption_algorithm="AES_GCM_V1",
        double_wrapping=True,
        plaintext_footer=False,
        data_key_length_bits=256,
    )
    return dataset.ParquetEncryptionConfig(
        make_crypto_factory(),
        KmsConnectionConfig(),
        encryption_configuration,
    )


def encryption_properties(
    column_names: list[str],
) -> parquet.FileEncryptionProperties:
    encryption_configuration = EncryptionConfiguration(
        footer_key=FOOTER_KEY_ID,
        column_keys={COLUMN_KEY_ID: column_names},
        encryption_algorithm="AES_GCM_V1",
        double_wrapping=True,
        plaintext_footer=False,
        data_key_length_bits=256,
    )
    return make_crypto_factory().file_encryption_properties(
        KmsConnectionConfig(),
        encryption_configuration,
    )


def run(input_data_path: Path, output_data_path: Path) -> None:
    try:
        logger.info(f"Encrypting parquet data from {input_data_path}")
        source_dataset = dataset.dataset(input_data_path)
        with parquet.ParquetWriter(
            output_data_path,
            source_dataset.schema,
            encryption_properties=encryption_properties(
                source_dataset.schema.names
            ),
        ) as writer:
            for batch in source_dataset.to_batches():
                writer.write_batch(batch)
        logger.info(f"Parquet encryption done {output_data_path}")
    except Exception as e:
        logger.error(f"Error during parquet encryption: {str(e)}")
        raise BuilderStepError("Failed to encrypt dataset") from e
