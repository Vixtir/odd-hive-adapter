import logging
from typing import Dict, Any, List, Tuple, Iterable

from more_itertools import flatten
from odd_contract.models import DataEntity

from mappers.metadata import MetadataExtractor
from mappers.columns.main import map_column
from mappers.oddrn import get_table_oddrn, get_owner_oddrn

HIVE_DATABASE_FIELD = "Database:           "
HIVE_OWNER_FIELD = "OwnerType:"
HIVE_CREATETIME_FIELD = "CreateTime:         "
HIVE_NUMROWS_FIELD = "numRows"
HIVE_COLUMN_NAME = "col_name"


def map_hive_table(
    host_name: str,
    raw_table_data: List[Tuple[str]],
    unmapped_column_stats: List[Dict[str, Any]],
    column_stats: Iterable[Tuple[str, Dict[str, Any]]],
    table_name: str,
) -> DataEntity:
    database_name = __get_value(raw_table_data, HIVE_DATABASE_FIELD)
    created_at = __get_value(raw_table_data, HIVE_CREATETIME_FIELD)
    owner = __get_value(raw_table_data, HIVE_OWNER_FIELD)

    table_oddrn = get_table_oddrn(database_name, table_name)
    owner_oddrn = get_owner_oddrn(host_name, owner)

    columns = flatten(
        [
            map_column(
                column_data, table_oddrn, __get_one_stat(column_stats, column_data)
            )
            for column_data in unmapped_column_stats
        ]
    )
    mt = MetadataExtractor()

    try:
        result = DataEntity.from_dict(
            {
                "oddrn": table_oddrn,
                "name": table_name,
                "owner": owner_oddrn,
                "updated_at": None,
                "created_at": created_at,
                "metadata": [mt.extract_dataset_metadata(raw_table_data)],
                "dataset": {
                    "parent_oddrn": None,
                    "description": None,
                    "subtype": "DATASET_TABLE",
                    "rows_number": __get_value(raw_table_data, HIVE_NUMROWS_FIELD),
                    "field_list": list(columns),
                },
            }
        )
        return result
    except (TypeError, KeyError):
        logging.warning(
            "Problems with DataEntity JSON serialization. " "Returning: {}."
        )
        return {}


def __get_value(raw_table_data, row_value) -> str:
    try:
        value = next(
            i[i.index(j) + 1] for i in raw_table_data for j in i if j == row_value
        )
        return value.strip()
    except StopIteration:
        logging.warning(
            "There was an error during getting value for row. "
            f'Value of "{row_value}" is not found.'
        )
        return None


def __get_one_stat(column_stats, column_data) -> Dict[str, Any]:
    try:
        return next(i[1] for i in column_stats if i[0] == column_data[HIVE_COLUMN_NAME])
    except (StopIteration, KeyError):
        logging.warning(
            "There was an exception raised during " "getting data for the column. "
        )
        return None
