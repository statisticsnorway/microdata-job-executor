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

ENHANCED_CSV_WITH_ENDING_DELIMITER_FILE = (
    f'{WORKING_DIR}/data_3_rows_with_ending_delimiter_enhanced.csv'
)
VALID_TEMPORAL_COVERAGE = {"start": 0, "stop": 30000}
INVALID_TEMPORAL_COVERAGE = {"start": 0, "stop": 2}
ENHANCED_CSV_FILES = [
    f'{WORKING_DIR}/data_3_rows_enhanced.csv',
    f'{WORKING_DIR}/data_4_rows_no_start_date_enhanced.csv',
    f'{WORKING_DIR}/data_4_rows_no_stop_date_enhanced.csv',
    f'{WORKING_DIR}/data_3_rows_instant_value_enhanced.csv'
]


def test_generate_epoch_day():
    date_since_epoch = dataset_enricher._generate_epoch_day(
        "1970-01-10", ['', r'\N', None]
    )
    assert date_since_epoch == '9'
    date_since_epoch = dataset_enricher._generate_epoch_day(
        "19700110", ['', r'\N', None]
    )
    assert date_since_epoch == '9'


def test_convert_from_csv_to_enhanced_csv():
    enhanced_csv_file_path = dataset_enricher.run(
        CSV_FILE, VALID_TEMPORAL_COVERAGE, "STRING"
    )
    file = open(enhanced_csv_file_path, 'r')
    lines = file.readlines()
    assert lines[0] == (
        "11111111501853;22048613;19720101;19721231;1972;730;1095\n"
    )
    assert lines[1] == (
        "11111111983265;1306729;19720101;19721231;1972;730;1095\n"
    )
    assert lines[2] == (
        "11111111864482;21529182;20170101;20171231;2017;17167;17531\n"
    )


def test_convert_from_csv_no_start_date_to_enhanced_csv():
    enhanced_csv_file_path = dataset_enricher.run(
        CSV_FILE_NO_START_DATE, VALID_TEMPORAL_COVERAGE, "STRING"
    )
    file = open(enhanced_csv_file_path, 'r')
    lines = file.readlines()
    assert lines[0] == (
        "11111111501853;22048613;19720101;19721231;1972;730;1095\n"
    )
    assert lines[1] == (
        "11111111983265;1306729;;19721231;;;1095\n"
    )
    assert lines[2] == (
        "11111111864482;21529182;20170101;20171231;2017;17167;17531\n"
    )
    assert lines[3] == (
        "11111111719226;21529182;;19721231;;;1095\n"
    )


def test_convert_from_csv_no_stop_date_to_enhanced_csv():
    enhanced_csv_file_path = dataset_enricher.run(
        CSV_FILE_NO_STOP_DATE, VALID_TEMPORAL_COVERAGE, "STRING"
    )
    file = open(enhanced_csv_file_path, 'r')
    lines = file.readlines()
    assert lines[0] == (
        "11111111501853;22048613;19720101;19721231;1972;730;1095\n"
    )
    assert lines[1] == (
        "11111111983265;1306729;19720101;;1972;730;\n"
    )
    assert lines[2] == (
        "11111111864482;21529182;20170101;20171231;2017;17167;17531\n"
    )
    assert lines[3] == (
        "11111111719226;21529182;20170101;;2017;17167;\n"
    )


def test_convert_from_csv_with_instant_to_enhanced_csv():
    enhanced_csv_file_path = dataset_enricher.run(
        CSV_WITH_INSTANT_VALUE, VALID_TEMPORAL_COVERAGE, "INSTANT"
    )
    file = open(enhanced_csv_file_path, 'r')
    lines = file.readlines()
    assert lines[0] == (
        "11111111501853;9;19720101;19721231;1972;730;1095\n"
    )
    assert lines[1] == (
        "11111111983265;10;19720101;19721231;1972;730;1095\n"
    )
    assert lines[2] == (
        "11111111864482;;20170101;20171231;2017;17167;17531\n"
    )


def test_convert_from_csv_to_enhanced_csv_bad_coverage():
    with pytest.raises(dataset_enricher.TemporalCoverageException) as e:
        dataset_enricher._enrich_csv(
            CSV_FILE, INVALID_TEMPORAL_COVERAGE, "STRING"
        )
    assert (
            "Date in dataset is outside of temporal coverage: '19720101'"
            in str(e.value)
    )


def test_convert_from_csv_with_ending_delimiter_to_enhanced_csv():
    enhanced_csv_file = dataset_enricher.run(
        CSV_WITH_ENDING_DELIMITER_FILE, VALID_TEMPORAL_COVERAGE, "STRING"
    )
    file = open(enhanced_csv_file, 'r')
    lines = file.readlines()
    assert lines[0] == (
        "11111111501853;22048613;19720101;19721231;1972;730;1095\n"
    )
    assert lines[1] == (
        "11111111983265;1306729;19720101;19721231;1972;730;1095\n"
    )
    assert lines[2] == (
        "11111111864482;21529182;20170101;20171231;2017;17167;17531\n"
    )


def teardown_function():
    for enhanced_csv_file in ENHANCED_CSV_FILES:
        try:
            os.remove(enhanced_csv_file)
        except OSError:
            pass
    try:
        os.remove(ENHANCED_CSV_WITH_ENDING_DELIMITER_FILE)
    except OSError:
        pass
