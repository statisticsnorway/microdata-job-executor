import logging
from pathlib import Path
from typing import List, Optional, Tuple, Union

import pyarrow
from pyarrow import dataset, compute, parquet

import microdata_tools
from microdata_tools.validation.model.metadata import UnitType

from job_executor.adapter import pseudonym_service
from job_executor.exception import BuilderStepError, UnregisteredUnitTypeError
from job_executor.model import Metadata

logger = logging.getLogger()

VALID_UNIT_ID_TYPES = ["FNR"]


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
    unit_id_type: Union[None, UnitType],
    job_id: str,
) -> Optional[pyarrow.Array]:
    """
    Pseudonymizes a column if a valid unit ID type is provided. Returns None otherwise.
    """
    if not unit_id_type or unit_id_type not in VALID_UNIT_ID_TYPES:
        return None

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
    identifier_unit_id_type: Optional[UnitType],
    measure_unit_id_type: Optional[UnitType],
    job_id: str,
) -> Path:
    input_dataset = dataset.dataset(input_parquet_path)

    unit_id_pseudonyms = _pseudonymize_column(
        input_dataset, "unit_id", identifier_unit_id_type, job_id
    )
    value_pseudonyms = _pseudonymize_column(
        input_dataset, "value", measure_unit_id_type, job_id
    )

    column_names = input_dataset.schema.names

    columns = []
    for column_name in column_names:
        if column_name == "unit_id" and unit_id_pseudonyms:
            columns.append(unit_id_pseudonyms)
            continue
        if column_name == "value" and value_pseudonyms:
            columns.append(value_pseudonyms)
            continue
        columns.append(
            input_dataset.to_table(columns=[column_name])[column_name]
        )

    pseudonymized_table = pyarrow.Table.from_arrays(columns, column_names)

    return pseudonymized_table


def run(input_parquet_path: Path, metadata: Metadata, job_id: str) -> Path:
    """
    Pseudonymizes the identifier & measure column of the dataset if.

    First extracts and validate the identifier unit type and measure unit type from
    the metadata using microdata_tools/validator.

    If valid unit types are provided, the unique values in the identifier & measure column
    are extracted and pseudonymized using the external pseudonym service.

    Finally all values in the identifier & measure column are replaced with the pseudonyms
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
    except UnregisteredUnitTypeError as e:
        raise BuilderStepError(
            f"Failed to pseudonymize, UnregisteredUnitType: {str(e)}"
        ) from e

    except Exception as e:
        logger.error(f"Error during pseudonymization: {str(e)}")
        raise BuilderStepError("Failed to pseudonymize dataset") from e
