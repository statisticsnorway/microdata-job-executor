import logging
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

from job_executor.exception import BuilderStepError


logger = logging.getLogger()


def _get_read_options():
    return pv.ReadOptions(
        skip_rows=0,
        encoding="utf8",
        column_names=[
            "unit_id", "value", "start", "stop",
            "start_year", "start_epoch_days", "stop_epoch_days"
        ]
    )


def _create_table(csv_convert_options: str, csv_parse_options: str,
                  data_path: str) -> pa.Table:
    table = pv.read_csv(
        input_file=data_path,
        read_options=_get_read_options(),
        parse_options=csv_parse_options,
        convert_options=csv_convert_options
    )
    return table


def _create_list_of_fields(data_type: str, partitioned: bool = False) -> list:
    types = dict(
        STRING=pa.string(),
        LONG=pa.int64(),
        DOUBLE=pa.float64(),
        INSTANT=pa.int64(),
        DATE=pa.int64()
    )
    if data_type.upper() not in types:
        raise ValueError(f'Unknown datatype {data_type}')
    fields = [
        pa.field(name='unit_id', type=pa.uint64(), nullable=False),
        pa.field(name='value', type=types[data_type.upper()], nullable=False),
        pa.field(name='start_epoch_days', type=pa.int16(), nullable=False),
        pa.field(name='stop_epoch_days', type=pa.int16(), nullable=False)
    ]
    if partitioned:
        start_year_field = [
            pa.field(name='start_year', type=pa.string(), nullable=True)
        ]
        fields = start_year_field + fields
    return fields


def _create_table_for_simple_parquet(data_path: str,
                                     data_type: str) -> pa.Table:
    data_schema = pa.schema(_create_list_of_fields(data_type))
    csv_convert_options = pv.ConvertOptions(
        column_types=data_schema,
        include_columns=[
            "unit_id", "value", "start_epoch_days", "stop_epoch_days"
        ]
    )
    return _create_table(
        csv_convert_options, pv.ParseOptions(delimiter=';'), data_path
    )


def _create_table_for_partitioned_parquet(data_path: str,
                                          data_type: str) -> pa.Table:
    data_schema = pa.schema(_create_list_of_fields(data_type, True))
    csv_convert_options = pv.ConvertOptions(
        column_types=data_schema,
        include_columns=[
            "unit_id", "value", "start_year",
            "start_epoch_days", "stop_epoch_days"
        ]
    )
    return _create_table(
        csv_convert_options, pv.ParseOptions(delimiter=';'), data_path
    )


def _convert_csv_to_simple_parquet(csv_data_path: str, data_type: str) -> str:
    parquet_file_path = csv_data_path.replace(
        '_enriched.csv', '__DRAFT.parquet'
    )
    logger.info(
        f"Converts csv {csv_data_path} "
        f"to simple parquet {parquet_file_path}"
    )
    table = _create_table_for_simple_parquet(csv_data_path, data_type)
    logger.info(f"Number of rows in parquet file: {table.num_rows}")

    pq.write_table(table, parquet_file_path)
    logger.info("Converted csv to simple parquet successfully")
    return parquet_file_path


def _convert_csv_to_partitioned_parquet(csv_data_path: str,
                                        data_type: str) -> str:
    parquet_partition_path = csv_data_path.replace(
        '_enriched.csv', '__DRAFT'
    )
    logger.info(
        f"Converts csv {csv_data_path} "
        f"to partitioned parquet {parquet_partition_path}"
    )

    table = _create_table_for_partitioned_parquet(csv_data_path, data_type)
    logger.info(f"Number of rows in parquet file: {table.num_rows}")

    metadata_collector = []
    pq.write_to_dataset(
        table,
        root_path=parquet_partition_path,
        partition_cols=['start_year'],
        metadata_collector=metadata_collector
    )
    logger.info("Converted csv to partitioned parquet successfully")
    return parquet_partition_path


def run(csv_data_path: str, temporality_type: str, data_type: str) -> str:
    """
    Converts a csv file to parquet format. Will partition the parquet
    if given temporality type is "STATUS" or "ACCUMULATED".
    """
    try:
        logger.info(
            f'''
            Converting {csv_data_path} to parquet
            data_type: {data_type}
            temporality_type: {temporality_type}
            '''
        )
        if temporality_type in ["STATUS", "ACCUMULATED"]:
            parquet_path = _convert_csv_to_partitioned_parquet(
                csv_data_path, data_type
            )
            logger.info(
                'Converted csv to partitioned parquet and wrote to '
                f'{parquet_path}'
            )
        else:
            parquet_path = _convert_csv_to_simple_parquet(
                csv_data_path, data_type
            )
            logger.info(
                'Converted csv to parquet and wrote to '
                f'{parquet_path}'
            )
        return parquet_path
    except Exception as e:
        logger.error(f'Error during conversion: {str(e)}')
        raise BuilderStepError('Failed to convert dataset') from e
