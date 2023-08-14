import logging
from pathlib import Path
from typing import List, Optional, Tuple, Union

import microdata_tools
import pyarrow
from pyarrow import dataset, compute, parquet

from job_executor.adapter import pseudonym_service
from job_executor.exception import BuilderStepError
from job_executor.model import Metadata

logger = logging.getLogger()


def _get_unit_types(
    metadata: Metadata,
) -> Tuple[Union[str, None], Union[str, None]]:
    return (
        metadata.get_identifier_key_type_name(),
        metadata.get_measure_key_type_name(),
    )


def _pseudonymize_column(
    input_dataset: dataset.FileSystemDataset,
    column_name: str,
    unit_id_type: str,
    job_id: str,
) -> pyarrow.Array:
    identifiers_table = input_dataset.to_table(columns=[column_name])
    identifiers_list = identifiers_table[column_name].to_pylist()
    unique_identifiers = compute.unique(
        identifiers_table[column_name]
    ).to_pylist()

    identifier_to_pseudonym = pseudonym_service.pseudonymize(
        unique_identifiers, unit_id_type, job_id
    )

    return [
        identifier_to_pseudonym[identifier] for identifier in identifiers_list
    ]


def _pseudonymize_if_needed(
    dataset: dataset.FileSystemDataset,
    column_name: str,
    unit_id_type: Optional[str],
    job_id: str,
) -> Optional[List[str]]:
    """
    Pseudonymizes a column if a valid unit ID type is provided. Returns None otherwise.
    """
    if not unit_id_type:
        return None

    return _pseudonymize_column(dataset, column_name, unit_id_type, job_id)


def _get_columns_excluding(
    dataset: dataset.FileSystemDataset, excluded_columns: List[str]
) -> List[str]:
    """
    Get a list of column names from the dataset excluding the specified columns.
    """
    return [
        name for name in dataset.schema.names if name not in excluded_columns
    ]


def _pseudonymize(
    input_parquet_path: Path,
    identifier_unit_id_type: Optional[str],
    measure_unit_id_type: Optional[str],
    job_id: str,
) -> Path:
    input_dataset = dataset.dataset(input_parquet_path)

    unit_id_pseudonyms = _pseudonymize_if_needed(
        input_dataset, "unit_id", identifier_unit_id_type, job_id
    )
    value_pseudonyms = _pseudonymize_if_needed(
        input_dataset, "value", measure_unit_id_type, job_id
    )

    excluded_columns = []
    if unit_id_pseudonyms:
        excluded_columns.append("unit_id")
    if value_pseudonyms:
        excluded_columns.append("value")
    column_names = _get_columns_excluding(input_dataset, excluded_columns)

    unprocessed_columns = input_dataset.to_table(columns=column_names)

    arrays_to_include = [
        col
        for col in [unit_id_pseudonyms, value_pseudonyms]
        if col is not None
    ]
    arrays_to_include.extend(
        [unprocessed_columns[name] for name in column_names]
    )

    # Construct the pseudonymized table
    pseudonymized_table = pyarrow.Table.from_arrays(
        arrays_to_include, input_dataset.schema.names
    )

    return pseudonymized_table


def run(input_parquet_path: Path, metadata: Metadata, job_id: str) -> Path:
    """
    Pseudonymizes the identifier column of the dataset. Requests pseudonyms
    from an external service and replaces all values in the identifier column.
    """
    try:
        logger.info(f"Pseudonymizing data {input_parquet_path}")
        identifier_unit_type, measure_unit_type = _get_unit_types(metadata)
        identifier_unit_id_type = (
            None
            if identifier_unit_type is None
            else microdata_tools.get_unit_id_type_for_unit_type(
                identifier_unit_type
            )
        )
        measure_unit_id_type = (
            None
            if measure_unit_type is None
            else microdata_tools.get_unit_id_type_for_unit_type(
                measure_unit_type
            )
        )
        pseudonymized_table = _pseudonymize(
            input_parquet_path,
            identifier_unit_id_type,
            measure_unit_id_type,
            job_id,
        )
        output_path = (
            input_parquet_path.parent
            / f"{input_parquet_path.stem}_pseudonymized.parquet"
        )

        parquet.write_table(pseudonymized_table, output_path)

        logger.info(f"Pseudonymization step done {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error during pseudonymization: {str(e)}")
        raise BuilderStepError("Failed to pseudonymize dataset") from e
