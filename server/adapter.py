import logging
from pyhive import hive

from typing import List, Dict, Any, Iterable
from more_itertools import flatten

from odd_contract.models import DataEntity

from mappers.tables import map_hive_table
from mappers.columns.main import map_column_stats
from mappers.consts import HIVE_COLUMN_NAME, HIVE_DATA_TYPE, HIVE_MIN, HIVE_MAX, HIVE_NULLS, \
    HIVE_DISTINCT, HIVE_MAX_LEN, \
    HIVE_AVG_LEN, HIVE_TRUE_COUNT, HIVE_FALSE_COUNT, HIVE_BIT_VECTOR, HIVE_COMMENT



class HiveAdapter:
    def __init__(
        self, host_name: str, auth: str, port=10000, user=None, password=None
    ) -> None:
        self.host_name = host_name
        if user and password:
            self._cur = hive.Connection(
                host=host_name, auth=auth, user=user, password=password, port=port
            ).cursor()
        self._cur = hive.Connection(host=host_name, auth=auth, port=port).cursor()

    def get_datasets(self) -> Iterable[DataEntity]:
        try:
            return list(
                flatten(
                    [self.__get_table_names(dn) for dn in self.__get_database_names()]
                )
            )
        except KeyError:
            logging.warning("No datasets found. Returning: [].")
            return []

    def __get_database_names(self) -> List[str]:
        """
        :return: ['default', 'test_db']
        """
        query = "SHOW DATABASES"
        self._cur.execute(query)
        fetches = self._cur.fetchall()
        return list(flatten(fetches))

    def __get_table_names(self, database_name: str) -> List[str]:
        """
        :return: ['hue__tmp_201801_punctuality_statistics_full_analysis', 'hue__tmp_netflix_titles']
        """
        query = f"USE {database_name}; SHOW TABLES"
        q_list = query.split(";")
        for q in q_list:
            self._cur.execute(q)
        fetches = self._cur.fetchall()
        tables_list = list(flatten(fetches))
        return [self.__process_table_raw_data(rt) for rt in tables_list]

    def __process_table_raw_data(self, table_name: str) -> DataEntity:
        query = f"DESCRIBE FORMATTED {table_name}"
        self._cur.execute(query)
        raw_table_data = self._cur.fetchall()  # -> List[tuple]
        column_names_dict = self.__get_column_names(table_name) # -> Dict
        unmapped_column_stats = list(
            (self.__get_stats_for_columns(c_name, c_type, table_name)
             for c_name, c_type in column_names_dict.items())
        )  # -> List[Dict[str: Any]]
        column_stats = map_column_stats(unmapped_column_stats)
        return map_hive_table(
            self.host_name,
            raw_table_data,
            unmapped_column_stats,
            column_stats,
            table_name,
        )

    def __get_column_names(self, table_name: str) -> List[str]:
        """
        :return: ['show_id', 'type', 'title', 'director', 'release_year', 'rating', 'description']
        """
        try:
            query = f"DESCRIBE {table_name}"
            self._cur.execute(query)
            raw_column_names_dict = self._cur.fetchall()
            result = {i[0]: i[1] for i in raw_column_names_dict}
            return result
        except KeyError:
            logging.warning("No column names found. Returning: {}.")
            return {}

    def __get_stats_for_columns(
        self, c_name: str, c_type: str, table_name: [Any]
    ) -> Dict[str, Any]:
        """
        :return: {'# col_name': 'airline_name', 'data_type': 'string',
        'min': '', 'max': '', 'num_nulls': '0', 'distinct_count': '290',
        'avg_col_len': '15.347568578553616', 'max_col_len': '38',
        'num_trues': '', 'num_falses': '', 'comment': 'from deserializer'}
        """
        if c_type.startswith('struct', 0):
            result = dict(zip((HIVE_COLUMN_NAME, HIVE_DATA_TYPE, HIVE_MIN, HIVE_MAX, HIVE_NULLS,
                               HIVE_DISTINCT, HIVE_AVG_LEN, HIVE_MAX_LEN, HIVE_TRUE_COUNT, HIVE_FALSE_COUNT,
                               HIVE_BIT_VECTOR, HIVE_COMMENT),
                              (c_name, c_type, '', '', '', '', '', '', '', '', '', '')))
            return result
        else:
            query = f"DESCRIBE FORMATTED {table_name} {c_name}"
            self._cur.execute(query)
            pre_result = self._cur.fetchall()
            try:
                return {i[0]: i[1] for i in pre_result}
            except AttributeError:
                logging.warning("No stats found. Returning: {}.")
                return {}
