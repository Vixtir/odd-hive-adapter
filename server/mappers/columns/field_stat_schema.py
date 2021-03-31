import re
from typing import Any, Dict

from odd_contract.models import (
    BooleanFieldStat,
    DateTimeFieldStat,
    NumberFieldStat,
    StringFieldStat,
    BinaryFieldStat,
)

DEFAULT_VALUE = -1


def _mapper_numeric(raw_column_stat: Dict[str, Any]):
    return {
        "low_value": _digit_checker(raw_column_stat["min"], float),
        "high_value": _digit_checker(raw_column_stat["max"], float),
        "mean_value": None,
        "median_value": None,
        "nulls_count": _digit_checker(raw_column_stat["num_nulls"], int),
        "unique_count": _digit_checker(raw_column_stat["distinct_count"], int),
    }


def _mapper_decimal(raw_column_stat: Dict[str, Any]):
    decimal_scale = re.search(r"\((.*?)\)", raw_column_stat["data_type"]).group(1)
    return {
        "low_value": DEFAULT_VALUE
        if (raw_column_stat["min"] == "")
        else _digit_checker(raw_column_stat["min"], float)
        / float(decimal_scale.replace(",", ".")),
        "high_value": DEFAULT_VALUE
        if (raw_column_stat["max"] == "")
        else _digit_checker(raw_column_stat["max"], float)
        / float(decimal_scale.replace(",", ".")),
        "mean_value": None,
        "median_value": None,
        "nulls_count": _digit_checker(raw_column_stat["num_nulls"], int),
        "unique_count": _digit_checker(raw_column_stat["distinct_count"], int),
    }


def _mapper_bytes(raw_column_stat: Dict[str, Any]):
    return {
        "max_length": _digit_checker(raw_column_stat["max_col_len"], int),
        "avg_length": _digit_checker(raw_column_stat["avg_col_len"], float),
        "nulls_count": _digit_checker(raw_column_stat["num_nulls"], int),
        "unique_count": _digit_checker(raw_column_stat["distinct_count"], int),
    }


def _mapper_boolean(raw_column_stat: Dict[str, Any]):
    return {
        "true_count": _digit_checker(raw_column_stat["num_trues"], int),
        "false_count": _digit_checker(raw_column_stat["num_falses"], int),
        "nulls_count": _digit_checker(raw_column_stat["num_nulls"], int),
    }


def _digit_checker(var, func):
    return func(var) if var.isdigit() else DEFAULT_VALUE


FIELD_TYPE_SCHEMA = {
    "boolean": {
        "odd_type": BooleanFieldStat,
        "field_name": "boolean_stats",
        "mapper": _mapper_boolean,
    },
    "date": {
        "odd_type": DateTimeFieldStat,
        "field_name": "date_time_stats",
        "mapper": _mapper_numeric,
    },
    "timestamp": {
        "odd_type": DateTimeFieldStat,
        "field_name": "date_time_stats",
        "mapper": _mapper_numeric,
    },
    "decimal": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_decimal,
    },
    "tinyint": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_numeric,
    },
    "smallint": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_numeric,
    },
    "int": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_numeric,
    },
    "bigint": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_numeric,
    },
    "float": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_numeric,
    },
    "double": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_numeric,
    },
    "string": {
        "odd_type": StringFieldStat,
        "field_name": "string_stats",
        "mapper": _mapper_bytes,
    },
    "varchar": {
        "odd_type": StringFieldStat,
        "field_name": "string_stats",
        "mapper": _mapper_bytes,
    },
    "char": {
        "odd_type": StringFieldStat,
        "field_name": "string_stats",
        "mapper": _mapper_bytes,
    },
    "binary": {
        "odd_type": BinaryFieldStat,
        "field_name": "binary_stats",
        "mapper": _mapper_bytes,
    },
}
