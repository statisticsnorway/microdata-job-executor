import json
import os
from pathlib import Path
import shutil
import pytest

from job_executor.exception import BuilderStepError
from job_executor.model import Metadata
from job_executor.worker.steps import dataset_pseudonymizer
from job_executor.adapter import pseudonym_service


WORKING_DIR = Path('tests/resources/worker/steps/pseudonymizer')
INPUT_CSV_PATH = WORKING_DIR / 'input.csv'
EXPECTED_OUTPUT_CSV_PATH = WORKING_DIR / 'expected_output.csv'
OUTPUT_CSV_PATH = WORKING_DIR / 'input_pseudonymized.csv'
JOB_ID = '123-123-123-123'
PSEUDONYM_DICT = {
    'i1': '1',
    'i2': '2',
    'i3': '3',
    'i4': '4'
}
with open(f'{WORKING_DIR}/metadata.json', encoding='utf-8') as file:
    METADATA = Metadata(**json.load(file))
with open(
    f'{WORKING_DIR}/metadata_invalid_unit_type.json', encoding='utf-8'
) as file:
    INVALID_METADATA = Metadata(**json.load(file))


@pytest.fixture(autouse=True)
def set_working_dir(monkeypatch):
    monkeypatch.setenv('WORKING_DIR', str(WORKING_DIR))


def setup_function():
    if os.path.isdir(f'{WORKING_DIR}_backup'):
        shutil.rmtree(f'{WORKING_DIR}_backup')

    shutil.copytree(WORKING_DIR, f'{WORKING_DIR}_backup')


def teardown_function():
    shutil.rmtree(WORKING_DIR)
    shutil.move(f'{WORKING_DIR}_backup', WORKING_DIR)


def test_pseudonymizer(mocker):
    mocker.patch.object(
        pseudonym_service,
        'pseudonymize',
        return_value=PSEUDONYM_DICT
    )
    assert (
        str(dataset_pseudonymizer.run(INPUT_CSV_PATH, METADATA, JOB_ID))
        == str(OUTPUT_CSV_PATH)
    )
    with open(OUTPUT_CSV_PATH, encoding='utf-8') as f:
        output_file = f.readlines()
    with open(EXPECTED_OUTPUT_CSV_PATH, encoding='utf-8') as f:
        expected_output_file = f.readlines()
    assert output_file == expected_output_file


def test_pseudonymizer_adapter_failure():
    with pytest.raises(BuilderStepError) as e:
        dataset_pseudonymizer.run(INPUT_CSV_PATH, METADATA, JOB_ID)
    assert 'Failed to pseudonymize dataset' == str(e.value)


def test_pseudonymizer_invalid_unit_id_type():
    with pytest.raises(BuilderStepError) as e:
        dataset_pseudonymizer.run(INPUT_CSV_PATH, INVALID_METADATA, JOB_ID)
    assert 'Failed to pseudonymize dataset' == str(e.value)
