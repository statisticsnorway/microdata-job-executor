from datetime import timedelta
from datetime import datetime
import logging

from job_executor.exception import BuilderStepError


logger = logging.getLogger()
EPOCH = datetime.utcfromtimestamp(0)
EMPTY_VALUES = ['', None]


def _generate_epoch_dict(temporal_coverage: dict) -> dict:
    days_since_epoch_for = {}
    start = EPOCH + timedelta(days=temporal_coverage['start'])
    stop = EPOCH + timedelta(days=temporal_coverage['stop'])
    logger.info(f"Generating epoch dict from {start} to {stop}")
    while start <= stop:
        days_since_epoch_for[start.strftime("%Y%m%d")] = (
            str((start - EPOCH).days)
        )
        start += timedelta(days=1)
    return days_since_epoch_for


def _generate_epoch_day(date: str) -> str:
    if date in EMPTY_VALUES:
        return ''
    else:
        datetime_obj = datetime.strptime(date.replace('-', ''), "%Y%m%d")
        return str((datetime_obj - EPOCH).days)


def _enrich_csv(
    input_csv_path: str,
    temporal_coverage: dict,
    data_type: str
) -> str:
    output_csv_path = input_csv_path.replace('.csv', '_enriched.csv')
    days_since_epoch_for = _generate_epoch_dict(temporal_coverage)
    try:
        target_file = open(output_csv_path, 'w', newline='', encoding='utf-8')
        with open(input_csv_path, newline='', encoding='utf-8') as csv_file:
            for line in csv_file:
                row = line.strip().split(';')
                # line_number: int = row[0]
                unit_id: str = row[1]
                value: str = row[2]
                start_date: str = row[3].replace('-', '')
                stop_date: str = row[4].replace('-', '')

                start_year = (
                    '' if start_date in EMPTY_VALUES else start_date[:4]
                )
                start_date_epoch_days = (
                    '' if start_date in EMPTY_VALUES
                    else days_since_epoch_for[start_date]
                )
                stop_date_epoch_days = (
                    '' if stop_date in EMPTY_VALUES
                    else days_since_epoch_for[stop_date]
                )
                if "INSTANT" == data_type.upper():
                    value = _generate_epoch_day(value)

                target_file.write(
                    ';'.join([
                        unit_id,
                        value,
                        start_year,
                        start_date_epoch_days,
                        stop_date_epoch_days
                    ]) + '\n'
                )
    except KeyError as e:
        raise TemporalCoverageException(  # pylint: disable=raise-missing-from
            f"Date in dataset is outside of temporal coverage: {e}"
        )
    target_file.close()
    return output_csv_path


def run(input_csv_path: str, temporal_coverage: dict, data_type: str) -> str:
    """
    Enriches a csv file for a dataset with extra columns of date information.
    Returns file path for enriched file.
    """
    try:
        logger.info(f'Enriching data {input_csv_path}')
        output_file = _enrich_csv(
            input_csv_path, temporal_coverage, data_type
        )
        logger.info(f'Enriched data and wrote to {output_file}')
        return output_file
    except Exception as e:
        logger.error(f'Error during enrichment: {str(e)}')
        raise BuilderStepError('Failed to enrich dataset') from e


class TemporalCoverageException(Exception):
    pass
