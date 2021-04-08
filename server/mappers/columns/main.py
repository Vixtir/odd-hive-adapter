import re
import logging
from typing import List, Tuple, Dict, Any, Iterable

from lark import Lark, LarkError

from .field_stat_schema import FIELD_TYPE_SCHEMA
from .hive_field_type_transformer import HiveFieldTypeTransformer
from mappers.consts import HIVE_FIELD_NAME, HIVE_COLUMN_NAME, HIVE_DATA_TYPE


hive_field_type_transformer = HiveFieldTypeTransformer()
parser = Lark.open(
    "hive_field_type_grammar.lark", rel_to=__file__, parser="lalr", start="type"
)

TYPES_HIVE_TO_ODD = {
    "int": "TYPE_INTEGER",
    "smallint": "TYPE_INTEGER",
    "tinyint": "TYPE_INTEGER",
    "double": "TYPE_NUMBER",
    "double precision": "TYPE_NUMBER",
    "bigint": "TYPE_NUMBER",
    "decimal": "TYPE_NUMBER",
    "float": "TYPE_NUMBER",
    "string": "TYPE_STRING",
    "varchar": "TYPE_STRING",
    "boolean": "TYPE_BOOLEAN",
    "char": "TYPE_CHAR",
    "date": "TYPE_DATETIME",
    "timestamp": "TYPE_DATETIME",
    "binary": "TYPE_BINARY",
    "array": "TYPE_LIST",
    "map": "TYPE_MAP",
    "struct": "TYPE_STRUCT",
    "uniontype": "TYPE_UNION",
}


def map_column_stats(
    unmapped_column_stats: List[Dict[str, Any]]
) -> Iterable[Tuple[str, Dict[str, Any]]]:
    """
    :return: [('airline_name', {
                         'string_stats': {
                                        'max_length': 4),
                                        'avg_length': 2.3),
                                        'nulls_count': 0),
                                        'unique_count': 24)
                                        }}),
     ('release_year', {
                        'string_stats': {
                                        'max_length': 4),
                                        'avg_length': 2.3),
                                        'nulls_count': 0),
                                        'unique_count': 24)
                                        }})
                                         ]
    """
    return [
        __map_column_stat(raw_column_stat) for raw_column_stat in unmapped_column_stats
    ]


def __map_column_stat(raw_column_stat: Dict[str, Any]) -> (str, Dict[str, Any]):
    """
    :return: {'# col_name': 'airline_name', 'data_type': 'string',
    'min': '', 'max': '', 'num_nulls': '0', 'distinct_count': '290',
    'avg_col_len': '15.347568578553616', 'max_col_len': '38',
    'num_trues': '', 'num_falses': '', 'comment': 'from deserializer'}
    """
    # TODO warning: parsing string and deleting contents of parentheses to
    #  get only varchar, char, decimal (i.e. not char(10), etc.)
    try:
        column_type_value = re.sub(r"\([^)]*\)", "", raw_column_stat[HIVE_DATA_TYPE])
        statistics_data = FIELD_TYPE_SCHEMA[column_type_value]
        mapper_fn = statistics_data["mapper"]
        return raw_column_stat[HIVE_COLUMN_NAME], {
            statistics_data[HIVE_FIELD_NAME]: mapper_fn(raw_column_stat)
        }
    except KeyError:
        return None, {}


def __parse(field_type: str) -> Dict[str, Any]:
    column_tree = parser.parse(field_type)
    return hive_field_type_transformer.transform(column_tree)


def map_column(
    raw_column_data: Dict[str, Any], table_oddrn: str, stats: Dict[str, Any]
) -> List[Dict[str, Any]]:
    try:
        column_type_value = re.sub(r"\([^)]*\)", "", raw_column_data[HIVE_DATA_TYPE])
        type_parsed = __parse(column_type_value)

        return __map_column(
            table_oddrn=table_oddrn,
            column_name=raw_column_data[HIVE_COLUMN_NAME],
            stats=stats,
            type_parsed=type_parsed,
        )
    except (LarkError, KeyError) as e:
        logging.warning(
            "There was an error during type parsing. "
            f'Column data: "{raw_column_data[HIVE_DATA_TYPE]}". '
            f'Table oddrn: "{table_oddrn}". '
            f'Reason: "{e}"'
        )
        return []


def __map_column(
    table_oddrn: str,
    parent_oddrn: str = None,
    column_name: str = None,
    stats: Dict[str, Any] = None,
    type_parsed: Dict[str, Any] = None,
    is_key: bool = None,
    is_value: bool = None,
) -> List[Dict[str, Any]]:
    result = []
    hive_type = type_parsed["type"]
    name = (
        column_name
        if column_name is not None
        else type_parsed["field_name"]
        if "field_name" in type_parsed
        else hive_type
    )

    resource_name = "keys" if is_key else "values" if is_value else "subcolumns"

    dsf = {
        "name": name,
        "oddrn": f"{table_oddrn}/columns/{name}"
        if parent_oddrn is None
        else f"{parent_oddrn}/{resource_name}/{name}",
        "parent_field_oddrn": parent_oddrn,
        "type": {
            "type": TYPES_HIVE_TO_ODD[hive_type],
            "logical_type": hive_type,
            "is_nullable": True,
        },
        "is_key": bool(is_key),
        "is_value": bool(is_value),
        "owner": None,
        "metadata": {},
        "stats": stats or {},
        "default_value": None,
        "description": None,
    }

    if hive_type in ["array", "struct", "union"]:
        for children in type_parsed["children"]:
            result.extend(
                __map_column(
                    table_oddrn=table_oddrn,
                    parent_oddrn=dsf["oddrn"],
                    type_parsed=children,
                )
            )

    if hive_type == "map":
        result.extend(
            __map_column(
                table_oddrn=table_oddrn,
                parent_oddrn=dsf["oddrn"],
                type_parsed=type_parsed["key_type"],
                is_key=True,
            )
        )

        result.extend(
            __map_column(
                table_oddrn=table_oddrn,
                parent_oddrn=dsf["oddrn"],
                type_parsed=type_parsed["value_type"],
                is_value=True,
            )
        )

    result.append(dsf)
    return result
