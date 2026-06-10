from pyarrow import dataset
from pyarrow.parquet.encryption import (
    DecryptionConfiguration,
    KmsConnectionConfig,
)

from job_executor.adapter.kms_client import make_crypto_factory


def decryption_file_format() -> dataset.ParquetFileFormat:
    decryption_config = dataset.ParquetDecryptionConfig(
        make_crypto_factory(),
        KmsConnectionConfig(),
        DecryptionConfiguration(),
    )
    scan_options = dataset.ParquetFragmentScanOptions(
        decryption_config=decryption_config
    )
    return dataset.ParquetFileFormat(default_fragment_scan_options=scan_options)
