import logging
from typing import Tuple, Union

from job_executor.exception import BuilderStepError
from job_executor.adapter import pseudonym_service
from job_executor.model import Metadata

logger = logging.getLogger()


def _get_unit_id_types(
    metadata: Metadata
) -> Tuple[Union[str, None], Union[str, None]]:
    return (
        metadata.get_identifier_key_type_name(),
        metadata.get_measure_key_type_name()
    )


def _pseudonymize_identifier_only(
    input_csv_path: str,
    unit_id_type: str,
    job_id: str
) -> str:
    unique_identifiers = set()
    with open(input_csv_path, newline='', encoding='utf8') as csv_file:
        for line in csv_file:
            unit_id = line.strip().split(';')[1]
            unique_identifiers.add(unit_id)
    identifier_to_pseudonym = pseudonym_service.pseudonymize(
        list(unique_identifiers), unit_id_type, job_id
    )
    output_csv_path = input_csv_path.replace('.csv', '_pseudonymized.csv')
    target_file = open(output_csv_path, 'w', newline='', encoding='utf-8')
    with open(input_csv_path, newline='', encoding='utf-8') as csv_file:
        for line in csv_file:
            row = line.strip().split(';')
            line_number: int = row[0]
            unit_id: str = row[1]
            value: str = row[2]
            start_date: str = row[3]
            stop_date: str = row[4]
            target_file.write(
                ';'.join([
                    line_number,
                    identifier_to_pseudonym[unit_id],
                    value,
                    start_date, stop_date
                ]) + '\n'
            )
    target_file.close()
    return output_csv_path


def _pseudonymize_measure_only(
    input_csv_path: str,
    unit_id_type: str,
    job_id: str
) -> str:
    unique_measure_values = set()
    with open(input_csv_path, newline='', encoding='utf-8') as csv_file:
        for line in csv_file:
            value = line.strip().split(';')[2]
            unique_measure_values.add(value)
    value_to_pseudonym = pseudonym_service.pseudonymize(
        list(unique_measure_values), unit_id_type, job_id
    )
    output_csv_path = input_csv_path.replace('.csv', '_pseudonymized.csv')
    target_file = open(output_csv_path, 'w', newline='', encoding='utf-8')
    with open(input_csv_path, newline='', encoding='utf-8') as csv_file:
        for line in csv_file:
            row = line.strip().split(';')
            line_number: int = row[0]
            unit_id: str = row[1]
            value: str = row[2]
            start_date: str = row[3]
            stop_date: str = row[4]
            target_file.write(
                ';'.join([
                    line_number,
                    unit_id,
                    value_to_pseudonym[value],
                    start_date, stop_date
                ]) + '\n'
            )
    target_file.close()
    return output_csv_path


def _pseudonymize_identifier_and_measure(
    input_csv_path: str,
    identifier_unit_id_type: str,
    measure_unit_id_type: str,
    job_id: str
) -> str:
    unique_idents = set()
    unique_measure_values = set()
    with open(input_csv_path, newline='', encoding='utf-8') as csv_file:
        for line in csv_file:
            row = line.strip().split(';')
            unit_id = row[1]
            value = row[2]
            unique_idents.add(unit_id)
            unique_measure_values.add(value)
    identifier_to_pseudonym = pseudonym_service.pseudonymize(
        list(unique_idents), identifier_unit_id_type, job_id
    )
    value_to_pseudonym = pseudonym_service.pseudonymize(
        list(unique_measure_values), measure_unit_id_type, job_id
    )
    output_csv_path = input_csv_path.replace('.csv', '_pseudonymized.csv')
    target_file = open(output_csv_path, 'w', newline='', encoding='utf-8')
    with open(input_csv_path, newline='', encoding='utf-8') as csv_file:
        for line in csv_file:
            row = line.strip().split(';')
            line_number: int = row[0]
            unit_id: str = row[1]
            value: str = row[2]
            start_date: str = row[3]
            stop_date: str = row[4]
            target_file.write(
                ';'.join([
                    line_number,
                    identifier_to_pseudonym[unit_id],
                    value_to_pseudonym[value],
                    start_date, stop_date
                ]) + '\n'
            )
    target_file.close()
    return output_csv_path


def _pseudonymize_csv(
    input_csv_path: str,
    identifier_unit_id_type: Union[str, None],
    measure_unit_id_type: Union[str, None],
    job_id: str
) -> str:
    if identifier_unit_id_type and not measure_unit_id_type:
        logger.info('Pseudonymizing identifier')
        return _pseudonymize_identifier_only(
            input_csv_path, identifier_unit_id_type, job_id
        )
    elif measure_unit_id_type and not identifier_unit_id_type:
        logger.info('Pseudonymizing measure')
        return _pseudonymize_measure_only(
            input_csv_path, measure_unit_id_type, job_id
        )
    elif identifier_unit_id_type and measure_unit_id_type:
        logger.info('Pseudonymizing identifier and measure')
        return _pseudonymize_identifier_and_measure(
            input_csv_path,
            identifier_unit_id_type,
            measure_unit_id_type,
            job_id
        )
    else:
        logger.info('No pseudonymization')
        return input_csv_path


def run(input_csv_path: str, metadata: Metadata, job_id: str) -> str:
    """
    Pseudonymizes the identifier column of the dataset. Requests pseudonyms
    from an external service and replaces all values in the identifier column.
    """
    try:
        logger.info(f'Pseudonymizing data {input_csv_path}')
        identifier_unit_id_type, measure_unit_id_type = (
            _get_unit_id_types(metadata)
        )
        output_file = _pseudonymize_csv(
            input_csv_path,
            identifier_unit_id_type,
            measure_unit_id_type,
            job_id
        )
        logger.info(f'Pseudonymization step done {output_file}')
        return output_file
    except Exception as e:
        logger.error(f'Error during pseudonymization: {str(e)}')
        raise BuilderStepError('Failed to pseudonymize dataset') from e