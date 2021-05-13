import logging
from time import perf_counter
from hive_metastore_client import HiveMetastoreClient
from typing import List, Dict, Any, Iterable
from more_itertools import flatten
import concurrent.futures
from odd_contract.models import DataEntity
from mappers.tables import map_hive_table
from mappers.columns.main import map_column_stats


class HiveAdapter:
    def __init__(self, host_name: str, port: int) -> None:
        self.host_name = host_name
        self.port = port
        self._connection = HiveMetastoreClient(host_name, port)
        self._cur = self._connection.open()

    def get_datasets(self) -> Iterable[DataEntity]:
        try:
            start = perf_counter()
            database_list = self._cur.get_all_databases()
            tables_list = list(flatten(self.__get_tables(db) for db in database_list))
            self.__disconnect()
            logging.info(f"Loaded DataEntities from database: {len(tables_list)} Datasets")
            logging.info(f"Elapsed time during the whole program in seconds: {perf_counter() - start}", )
            return tables_list
        except KeyError:
            logging.warning("No datasets found. Returning: [].")
            return []

    def __get_tables(self, db: str) -> List[DataEntity]:
        tables_list = self._cur.get_all_tables(db)
        if tables_list:
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = []
                aggregated_table_stats = []
                for table in tables_list:
                    futures.append(executor.submit(self._cur.get_table, db, table))
                for future in concurrent.futures.as_completed(futures):
                    future_result = future.result()
                    aggregated_table_stats.append(future_result)
            output = [self.__process_table_raw_data(table_stats) for table_stats in aggregated_table_stats]
            return output

        else:
            return []

    def __process_table_raw_data(self, table_stats) -> DataEntity:
        columns = {c.name: c.type for c in table_stats.sd.cols}
        unmapped_stats = self.__get_columns_stats(table_stats, columns)
        stats = map_column_stats(unmapped_stats) or None
        result = map_hive_table(self.host_name, table_stats, columns, stats)
        return result

    def __get_columns_stats(self, table_stats, columns: Dict) -> List :
        if table_stats.parameters.get('COLUMN_STATS_ACCURATE', None):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as columns_executor:
                stats = []
                futures = []
                for column_name in columns.keys():
                    futures.append(columns_executor.submit(self._cur.get_table_column_statistics,
                                                           table_stats.dbName,
                                                           table_stats.tableName,
                                                           column_name))
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future_result = future.result()

                    except Exception as exc:
                        logging.warning(f"Can't retrieve statistics for '{column_name}' in '{table_stats.tableName}'. "
                                        f"Generated an exception: {exc}. "
                                        f"May be the type: array, struct, map, union")
                    else:
                        stats.append(future_result)
            return stats
        else:
            logging.info(f"Table statistics for '{table_stats.tableName}' is not available. "
                         f"Stats has not been gathered. ")
            return []

    def __disconnect(self):
        try:
            if self._cur:
                self._cur.close()
                logging.info("Connection is closed")
        except Exception as e:
            logging.warning(f"Generated an exception in __disconnect(): {e}. ")
