import logging
from pathlib import Path
from typing import Tuple, Union

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
    unique_identifiers = compute.unique(
        identifiers_table[column_name]
    ).to_pylist()
    identifier_to_pseudonym = pseudonym_service.pseudonymize(
        unique_identifiers, unit_id_type, job_id
    )
    return [
        identifier_to_pseudonym[identifier]
        for identifier in identifiers_table[column_name].to_pylist()
    ]


def _pseudonymize(
    input_parquet_path: Path,
    identifier_unit_id_type: Union[str, None],
    measure_unit_id_type: Union[str, None],
    job_id: str,
) -> Path:
    # TODO: consider rewriting more explicit easier to read code
    input_dataset = dataset.dataset(input_parquet_path)
    unit_id_pseudonyms = (
        None
        if identifier_unit_id_type is None
        else _pseudonymize_column(
            input_dataset, "unit_id", identifier_unit_id_type, job_id
        )
    )
    value_pseudonyms = (
        None
        if measure_unit_id_type is None
        else _pseudonymize_column(
            input_dataset, "value", measure_unit_id_type, job_id
        )
    )
    column_names = input_dataset.schema.names
    if unit_id_pseudonyms:
        column_names = [name for name in column_names if name != "unit_id"]
    if value_pseudonyms:
        column_names = [name for name in column_names if name != "values"]
    unprocessed_columns = input_dataset.to_table(columns=column_names)
    pseudonymized_table = pyarrow.Table.from_arrays(
        [
            column
            for column in [
                unit_id_pseudonyms,
                value_pseudonyms,
                *[unprocessed_columns[name] for name in column_names],
            ]
            if column is not None
        ],
        input_dataset.schema.names,
    )
    output_path = (
        input_parquet_path.parent
        / f"{str(input_parquet_path.stem)}_pseudonymized.parquet"
    )
    parquet.write_table(pseudonymized_table, output_path)
    return output_path


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
        output_file = _pseudonymize(
            input_parquet_path,
            identifier_unit_id_type,
            measure_unit_id_type,
            job_id,
        )
        logger.info(f"Pseudonymization step done {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"Error during pseudonymization: {str(e)}")
        raise BuilderStepError("Failed to pseudonymize dataset") from e
