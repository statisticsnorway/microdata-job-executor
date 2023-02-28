# pylint: disable=protected-access
import os
import pytest

from job_executor.worker.steps import dataset_enricher

WORKING_DIR = 'tests/resources/worker/steps/enricher'

CSV_FILE = f'{WORKING_DIR}/data_3_rows.csv'
CSV_FILE_NO_START_DATE = f'{WORKING_DIR}/data_4_rows_no_start_date.csv'
CSV_FILE_NO_STOP_DATE = f'{WORKING_DIR}/data_4_rows_no_stop_date.csv'
CSV_WITH_ENDING_DELIMITER_FILE = (
    f'{WORKING_DIR}/data_3_rows_with_ending_delimiter.csv'
)
CSV_WITH_INSTANT_VALUE = f'{WORKING_DIR}/data_3_rows_instant_value.csv'

ENRICHED_CSV_WITH_ENDING_DELIMITER_FILE = (
    f'{WORKING_DIR}/data_3_rows_with_ending_delimiter_enriched.csv'
)
VALID_TEMPORAL_COVERAGE = {"start": 0, "stop": 30000}
INVALID_TEMPORAL_COVERAGE = {"start": 0, "stop": 2}
ENRICHED_CSV_FILES = [
    f'{WORKING_DIR}/data_3_rows_enriched.csv',
    f'{WORKING_DIR}/data_4_rows_no_start_date_enriched.csv',
    f'{WORKING_DIR}/data_4_rows_no_stop_date_enriched.csv',
    f'{WORKING_DIR}/data_3_rows_instant_value_enriched.csv'
]


def test_generate_epoch_day():
    date_since_epoch = dataset_enricher._generate_epoch_day('1970-01-10')
    assert date_since_epoch == '9'
    date_since_epoch = dataset_enricher._generate_epoch_day('19700110')
    assert date_since_epoch == '9'


def test_convert_from_csv_to_enriched_csv():
    enriched_csv_file_path = dataset_enricher.run(
        CSV_FILE, VALID_TEMPORAL_COVERAGE, "STRING"
    )
    file = open(enriched_csv_file_path, 'r', encoding='utf8')
    lines = file.readlines()
    assert lines[0] == (
        "11111111501853;22048613;1972;730;1095\n"
    )
    assert lines[1] == (
        "11111111983265;1306729;1972;730;1095\n"
    )
    assert lines[2] == (
        "11111111864482;21529182;2017;17167;17531\n"
    )


def test_convert_from_csv_no_start_date_to_enriched_csv():
    enriched_csv_file_path = dataset_enricher.run(
        CSV_FILE_NO_START_DATE, VALID_TEMPORAL_COVERAGE, "STRING"
    )
    file = open(enriched_csv_file_path, 'r', encoding='utf8')
    lines = file.readlines()
    assert lines[0] == (
        "11111111501853;22048613;1972;730;1095\n"
    )
    assert lines[1] == (
        "11111111983265;1306729;;;1095\n"
    )
    assert lines[2] == (
        "11111111864482;21529182;2017;17167;17531\n"
    )
    assert lines[3] == (
        "11111111719226;21529182;;;1095\n"
    )


def test_convert_from_csv_no_stop_date_to_enriched_csv():
    enriched_csv_file_path = dataset_enricher.run(
        CSV_FILE_NO_STOP_DATE, VALID_TEMPORAL_COVERAGE, "STRING"
    )
    file = open(enriched_csv_file_path, 'r', encoding='utf-8')
    lines = file.readlines()
    assert lines[0] == (
        "11111111501853;22048613;1972;730;1095\n"
    )
    assert lines[1] == (
        "11111111983265;1306729;1972;730;\n"
    )
    assert lines[2] == (
        "11111111864482;21529182;2017;17167;17531\n"
    )
    assert lines[3] == (
        "11111111719226;21529182;2017;17167;\n"
    )


def test_convert_from_csv_with_instant_to_enriched_csv():
    enriched_csv_file_path = dataset_enricher.run(
        CSV_WITH_INSTANT_VALUE, VALID_TEMPORAL_COVERAGE, "INSTANT"
    )
    file = open(enriched_csv_file_path, 'r', encoding='utf-8')
    lines = file.readlines()
    assert lines[0] == (
        "11111111501853;9;1972;730;1095\n"
    )
    assert lines[1] == (
        "11111111983265;10;1972;730;1095\n"
    )
    assert lines[2] == (
        "11111111864482;;2017;17167;17531\n"
    )


def test_convert_from_csv_to_enriched_csv_bad_coverage():
    with pytest.raises(dataset_enricher.TemporalCoverageException) as e:
        dataset_enricher._enrich_csv(
            CSV_FILE, INVALID_TEMPORAL_COVERAGE, "STRING"
        )
    assert (
            "Date in dataset is outside of temporal coverage: '19720101'"
            in str(e.value)
    )


def test_convert_from_csv_with_ending_delimiter_to_enriched_csv():
    enriched_csv_file = dataset_enricher.run(
        CSV_WITH_ENDING_DELIMITER_FILE, VALID_TEMPORAL_COVERAGE, "STRING"
    )
    file = open(enriched_csv_file, 'r', encoding='utf-8')
    lines = file.readlines()
    assert lines[0] == (
        "11111111501853;22048613;1972;730;1095\n"
    )
    assert lines[1] == (
        "11111111983265;1306729;1972;730;1095\n"
    )
    assert lines[2] == (
        "11111111864482;21529182;2017;17167;17531\n"
    )


def teardown_function():
    for enriched_csv_file in ENRICHED_CSV_FILES:
        try:
            os.remove(enriched_csv_file)
        except OSError:
            pass
    try:
        os.remove(ENRICHED_CSV_WITH_ENDING_DELIMITER_FILE)
    except OSError:
        pass
