import logging
from typing import Dict, Any, List, Iterable
from datetime import datetime
from more_itertools import flatten
from odd_contract.models import DataEntity
from mappers.metadata import _metadata
from mappers.columns.main import map_column
from mappers.oddrn import get_table_oddrn, get_owner_oddrn
from mappers import TableNamedTuple, ColumnsNamedTuple, StatsNamedTuple


def map_hive_table(host_name: str, table_stats, columns: dict, stats: List[StatsNamedTuple] = None) -> DataEntity:
    table_oddrn = get_table_oddrn(table_stats.dbName, table_stats.tableName)
    owner_oddrn = get_owner_oddrn(host_name, table_stats.owner)
    created_at = f'{datetime.fromtimestamp(table_stats.createTime)}'
    updated_at = f'{datetime.fromtimestamp(table_stats.lastAccessTime)}'
    columns_mapping = list(flatten([map_column(c_name, c_type, table_oddrn, __get_stats(c_name, stats))
                                    for c_name, c_type in columns.items()]))
    try:
        result = DataEntity.from_dict(
            {
                "oddrn": table_oddrn,
                "name": table_stats.tableName,
                "owner": owner_oddrn,
                "updated_at": updated_at,
                "created_at": created_at,
                "metadata": [_metadata(table_stats)],
                "dataset": {
                    "parent_oddrn": None,
                    "description": None,
                    "subtype": "DATASET_TABLE",
                    "rows_number": table_stats.parameters.get('numRows', None),
                    "field_list": columns_mapping or [],
                },
            }
        )
        return result
    except Exception as e:
        logging.warning(f"Can't build DataEntity for '{table_stats.tableName}' in '{table_stats.dbName}' database. "
                        f"Exception raised: {e}")
        return {}


def __get_stats(c_name: str, stats):
    try:
        if stats:
            result = next(i[1] for i in stats if c_name == i[0])
            print(f' __get_stats {result}')
            return result
    except (StopIteration, TypeError, KeyError):
        logging.info(f"There was an exception raised when extracting column stats for '{c_name}'.")
        return None
