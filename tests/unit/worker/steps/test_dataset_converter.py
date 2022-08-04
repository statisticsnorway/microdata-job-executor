# pylint: disable=protected-access
import os.path
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from job_executor.worker.steps import dataset_converter

WORKING_DIR = 'tests/resources/worker/steps/converter'
DATASET_NAME = 'KREFTREG_DS'
CSV_FILE_READY_FOR_PARQUET_CONVERSION = (
    f'{WORKING_DIR}/KREFTREG_DS_enriched.csv'
)
OUTPUT_PARQUET_FILE = f'{os.environ["WORKING_DIR"]}/KREFTREG_DS__DRAFT.parquet'
OUTPUT_PARTITIONED_PARQUET_DIR = (
    f'{os.environ["WORKING_DIR"]}/KREFTREG_DS__DRAFT'
)


def test_create_list_of_fields_for_simple_parquet():
    fields = dataset_converter._create_list_of_fields("String")
    assert fields[0].name == 'unit_id'
    assert fields[0].type == pa.uint64()
    assert fields[1].name == 'value'
    assert fields[1].type == pa.string()
    assert fields[2].name == 'start_epoch_days'
    assert fields[2].type == pa.int16()
    assert fields[3].name == 'stop_epoch_days'
    assert fields[3].type == pa.int16()
    assert len(fields) == 4

    fields = dataset_converter._create_list_of_fields("Long")
    assert fields[1].type == pa.int64()

    fields = dataset_converter._create_list_of_fields("Double")
    assert fields[1].type == pa.float64()

    fields = dataset_converter._create_list_of_fields("Instant")
    assert fields[1].type == pa.int64()

    with pytest.raises(ValueError):
        dataset_converter._create_list_of_fields("UNKNOWN")


def test_create_list_of_fields_for_partitioned_parquet():
    fields = dataset_converter._create_list_of_fields("String", True)
    assert fields[0].name == 'start_year'
    assert fields[0].type == pa.string()
    assert fields[1].name == 'unit_id'
    assert fields[1].type == pa.uint64()
    assert fields[2].name == 'value'
    assert fields[2].type == pa.string()
    assert fields[3].name == 'start_epoch_days'
    assert fields[3].type == pa.int16()
    assert fields[4].name == 'stop_epoch_days'
    assert fields[4].type == pa.int16()
    assert len(fields) == 5

    fields = dataset_converter._create_list_of_fields("Long", True)
    assert fields[2].type == pa.int64()

    fields = dataset_converter._create_list_of_fields("Double", True)
    assert fields[2].type == pa.float64()

    fields = dataset_converter._create_list_of_fields("Instant", True)
    assert fields[2].type == pa.int64()

    with pytest.raises(ValueError):
        dataset_converter._create_list_of_fields("UNKNOWN", True)


def test_convert_from_csv_with_string_value_to_simple_parquet():
    data_type = "String"
    output_parquet_file = (
        dataset_converter.run(
            DATASET_NAME,
            CSV_FILE_READY_FOR_PARQUET_CONVERSION,
            temporality_type="EVENT",
            data_type=data_type
        )
    )
    parquet_file = pq.read_table(output_parquet_file)
    verify_parquet_file_schema(parquet_file, data_type)
    assert output_parquet_file.endswith('.parquet')
    assert 3 == parquet_file.num_rows


def test_convert_from_csv_with_long_value_to_simple_parquet():
    data_type = "Long"
    output_parquet_file = (
        dataset_converter.run(
            DATASET_NAME,
            CSV_FILE_READY_FOR_PARQUET_CONVERSION,
            temporality_type="EVENT",
            data_type=data_type
        )
    )
    parquet_file = pq.read_table(output_parquet_file)
    verify_parquet_file_schema(parquet_file, data_type)
    assert output_parquet_file.endswith('.parquet')
    assert 3 == parquet_file.num_rows


def test_convert_from_csv_with_double_value_to_simple_parquet():
    data_type = "Double"
    output_parquet_file = (
        dataset_converter.run(
            DATASET_NAME,
            CSV_FILE_READY_FOR_PARQUET_CONVERSION,
            temporality_type="EVENT",
            data_type=data_type
        )
    )
    parquet_file = pq.read_table(output_parquet_file)
    verify_parquet_file_schema(parquet_file, data_type)
    assert output_parquet_file.endswith('.parquet')
    assert 3 == parquet_file.num_rows


def test_convert_from_csv_with_date_value_to_simple_parquet():
    data_type = "Instant"
    output_parquet_file = (
        dataset_converter.run(
            DATASET_NAME,
            CSV_FILE_READY_FOR_PARQUET_CONVERSION,
            temporality_type="EVENT",
            data_type=data_type
        )
    )
    parquet_file = pq.read_table(output_parquet_file)
    verify_parquet_file_schema(parquet_file, data_type)
    assert output_parquet_file.endswith('.parquet')
    assert 3 == parquet_file.num_rows


def test_convert_from_csv_to_partitioned_parquet():
    data_type = "String"
    output_partitioned_parquet_dir = (
        dataset_converter.run(
            DATASET_NAME,
            CSV_FILE_READY_FOR_PARQUET_CONVERSION,
            temporality_type="STATUS",
            data_type=data_type
        )
    )
    assert not output_partitioned_parquet_dir.endswith('.parquet')
    assert output_partitioned_parquet_dir == (
        f'{os.environ["WORKING_DIR"]}/{DATASET_NAME}__DRAFT'
    )
    partitioned_parquet = pq.read_table(output_partitioned_parquet_dir)
    verify_partition_schema(partitioned_parquet, data_type)
    verify_partition_rows_and_columns(partitioned_parquet)

    partition_dir_1972 = f'{output_partitioned_parquet_dir}/start_year=1972'
    partition_dir_2017 = f'{output_partitioned_parquet_dir}/start_year=2017'
    verify_that_subdirectories_exist(
        partition_dir_1972, partition_dir_2017
    )
    verify_subdirectories_have_one_file_each(
        partition_dir_1972, partition_dir_2017
    )
    parquet_file_1972_path = (
        f'{partition_dir_1972}/{os.listdir(partition_dir_1972)[0]}'
    )
    partitioned_parquet_1972_file = pq.read_table(parquet_file_1972_path)

    verify_parquet_file_schema(partitioned_parquet_1972_file, data_type)
    assert 2 == partitioned_parquet_1972_file.num_rows


def verify_parquet_file_schema(partitioned_parquet_file, data_type: str):
    schema = partitioned_parquet_file.schema
    assert_all_except_start_year(data_type, schema)
    assert 0 == schema.get_field_index('unit_id')
    assert 1 == schema.get_field_index('value')
    assert 2 == schema.get_field_index('start_epoch_days')
    assert 3 == schema.get_field_index('stop_epoch_days')
    assert partitioned_parquet_file.column_names == [
        'unit_id', 'value', 'start_epoch_days', 'stop_epoch_days'
    ]


def verify_partition_schema(partitioned_parquet, data_type: str):
    schema = partitioned_parquet.schema
    assert_all_except_start_year(data_type, schema)
    assert 'int32' == (
        schema.field('start_year').type.value_type
    )
    assert 0 == schema.get_field_index('unit_id')
    assert 1 == schema.get_field_index('value')
    assert 2 == schema.get_field_index('start_epoch_days')
    assert 3 == schema.get_field_index('stop_epoch_days')
    assert 4 == schema.get_field_index('start_year')


def assert_all_except_start_year(data_type, schema):
    assert 'uint64' == schema.field('unit_id').type
    if data_type == "STRING":
        assert 'string' == schema.field('value').type
    elif data_type == "LONG":
        assert 'int64' == schema.field('value').type
    elif data_type == "DOUBLE":
        assert 'float64' == schema.field('value').type
    elif data_type == "DATE":
        assert 'int64' == schema.field('value').type
    assert 'int16' == schema.field('start_epoch_days').type
    assert 'int16' == schema.field('stop_epoch_days').type


def verify_subdirectories_have_one_file_each(partition_dir_1972,
                                             partition_dir_2017):
    assert 1 == len(os.listdir(partition_dir_1972))
    assert 1 == len(os.listdir(partition_dir_2017))


def verify_that_subdirectories_exist(partition_dir_1972, partition_dir_2017):
    assert os.path.isdir(partition_dir_1972)
    assert os.path.isdir(partition_dir_2017)


def verify_partition_rows_and_columns(partitioned_parquet):
    assert 3 == partitioned_parquet.num_rows
    assert partitioned_parquet.column_names == [
        'unit_id', 'value', 'start_epoch_days',
        'stop_epoch_days', 'start_year'
    ]


def teardown_function():
    try:
        os.remove(OUTPUT_PARQUET_FILE)
    except OSError:
        pass
    try:
        shutil.rmtree(OUTPUT_PARTITIONED_PARQUET_DIR)
    except OSError:
        pass
