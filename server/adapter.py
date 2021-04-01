import logging
from pyhive import hive

from typing import List, Dict, Any, Iterable
from more_itertools import flatten

from odd_contract.models import DataEntity

from mappers.tables import map_hive_table
from mappers.columns.main import map_column_stats


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
        clean_column_names = self.__get_column_names(table_name)
        unmapped_column_stats = [
            (self.__get_stats_for_columns(column_name, table_name))
            for column_name in clean_column_names
        ]  # -> List[Dict[str: Any]]
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
            query = f"SHOW COLUMNS IN {table_name}"
            self._cur.execute(query)
            raw_column_names = self._cur.fetchall()
            return [cn[0] for cn in raw_column_names]
        except KeyError:
            logging.warning("No column names found. Returning: [].")
            return []

    def __get_stats_for_columns(
        self, column_name: str, table_name: [Any]
    ) -> Dict[str, Any]:
        """
        :return: {'# col_name': 'airline_name', 'data_type': 'string',
        'min': '', 'max': '', 'num_nulls': '0', 'distinct_count': '290',
        'avg_col_len': '15.347568578553616', 'max_col_len': '38',
        'num_trues': '', 'num_falses': '', 'comment': 'from deserializer'}
        """
        query = f"DESCRIBE FORMATTED {table_name} {column_name}"
        # TODO check "date" and "array" column formats
        if column_name == "date" or column_name == "cast":
            zipped = {}
        else:
            self._cur.execute(query)
            pre_result = self._cur.fetchall()
            zipped = dict(zip(pre_result[0], pre_result[2]))
        return {k.strip(): v for k, v in zipped.items()}

        # query = f"DESCRIBE FORMATTED {table_name} {column_name}"
        # # TODO check "date" and "array" column formats
        # self._cur.execute(query)
        # pre_result = self._cur.fetchall()
        # zipped = dict(zip(pre_result[0], pre_result[2]))
        # return {k.strip(): v for k, v in zipped.items()}
