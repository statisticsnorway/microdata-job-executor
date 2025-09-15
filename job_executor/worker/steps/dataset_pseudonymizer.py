import logging
from pathlib import Path

import microdata_tools
import pyarrow
from microdata_tools.validation.exceptions import UnregisteredUnitTypeError
from microdata_tools.validation.model.metadata import UnitIdType, UnitType
from pyarrow import compute, dataset, parquet

from job_executor.adapter import pseudonym_service
from job_executor.exception import BuilderStepError
from job_executor.model import Metadata

logger = logging.getLogger()


def _get_unit_types(
    metadata: Metadata,
) -> tuple[str | None, str | None]:
    """
    Extracts the identifier & measure unit type from the metadata.
    """
    return (
        metadata.get_identifier_key_type_name(),
        metadata.get_measure_key_type_name(),
    )


def _fetch_column_pseudonyms(
    input_dataset: dataset.FileSystemDataset,
    column_name: str,
    unit_id_type: UnitIdType | None,
    job_id: str,
) -> list[str] | None:
    """
    Pseudonymizes a column if a pseudonymizable unit ID type is provided.
    Returns None otherwise.
    """
    if not unit_id_type:
        return None

    identifiers_table = input_dataset.to_table(columns=[column_name])

    string_identifiers = identifiers_table[column_name].cast(pyarrow.string())
    unique_identifiers = compute.unique(string_identifiers).to_pylist()  # type: ignore

    identifier_to_pseudonym = pseudonym_service.pseudonymize(
        unique_identifiers, unit_id_type, job_id
    )

    identifiers_list = string_identifiers.to_pylist()
    pseudonymized_data = [
        identifier_to_pseudonym[identifier] for identifier in identifiers_list
    ]

    return pseudonymized_data


def _get_column_pseudonyms_array(
    input_dataset: dataset.FileSystemDataset,
    column_name: str,
    unit_id_type: UnitIdType | None,
    job_id: str,
) -> pyarrow.Array:
    """
    Pseudonymizes a column if a pseudonymizable unit ID type is provided.
    Returns the original column otherwise.
    """
    pseudonyms = _fetch_column_pseudonyms(
        input_dataset, column_name, unit_id_type, job_id
    )

    if pseudonyms:
        return pyarrow.array(pseudonyms).cast(pyarrow.int64())
    else:
        # get logical type of column
        column_type = input_dataset.schema.field(column_name).type
        # cast column to logical type - just to be safe
        return input_dataset.to_table(columns=[column_name])[column_name].cast(
            column_type
        )


def _get_regular_column(
    input_dataset: dataset.FileSystemDataset,
    column_name: str,
    arrow_type: pyarrow.DataType,
) -> pyarrow.Array:
    """
    Returns a column as is, casting it to the provided arrow type.
    """
    return input_dataset.to_table(columns=[column_name])[column_name].cast(
        arrow_type
    )


def _pseudonymize(
    input_parquet_path: Path,
    identifier_unit_id_type: UnitIdType | None,
    measure_unit_id_type: UnitIdType | None,
    job_id: str,
) -> pyarrow.Table:
    input_dataset = dataset.dataset(input_parquet_path)
    columns = []

    # Handle potential pseudonymized columns
    columns.append(
        _get_column_pseudonyms_array(
            input_dataset, "unit_id", identifier_unit_id_type, job_id
        )
    )
    columns.append(
        _get_column_pseudonyms_array(
            input_dataset, "value", measure_unit_id_type, job_id
        )
    )

    # Handle regular columns
    columns.append(
        _get_regular_column(input_dataset, "start_epoch_days", pyarrow.int16())
    )
    columns.append(
        _get_regular_column(input_dataset, "stop_epoch_days", pyarrow.int16())
    )
    if "start_year" in input_dataset.schema.names:
        columns.append(
            _get_regular_column(input_dataset, "start_year", pyarrow.string())
        )

    pseudonymized_table = pyarrow.Table.from_arrays(
        columns, input_dataset.schema.names
    )

    return pseudonymized_table


def run(input_parquet_path: Path, metadata: Metadata, job_id: str) -> Path:
    """
    Pseudonymizes the identifier & measure column of the dataset if.

    First extracts and validate the identifier unit type and measure unit type
    from the metadata using microdata_tools/validator.

    If valid unit types are provided, the unique values in the identifier &
    measure column are extracted and pseudonymized using the external
    pseudonym service.

    Finally all values in the identifier & measure column are replaced with
    the pseudonyms.
    """
    try:
        logger.info(f"Pseudonymizing data {input_parquet_path}")

        identifier_unit_type, measure_unit_type = _get_unit_types(metadata)
        identifier_unit_id_type = (
            None
            if identifier_unit_type is None
            else microdata_tools.get_unit_id_type_for_unit_type(
                UnitType(identifier_unit_type)
            )
        )
        measure_unit_id_type = (
            None
            if measure_unit_type is None
            else microdata_tools.get_unit_id_type_for_unit_type(
                UnitType(measure_unit_type)
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
        logger.exception("Error stacktrace during pseudonymization", exc_info=e)
        logger.error(f"Error during pseudonymization: {str(e)}")
        raise BuilderStepError("Failed to pseudonymize dataset") from e
