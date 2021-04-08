import logging
from typing import Any, Dict, Set, List
from mappers.consts import HIVE_DETAILED_TABLE_INFO_FIELD, \
    HIVE_TABLE_PARAMETERS_FIELD, \
    HIVE_STORAGE_INFORMATION_FIELD,\
    HIVE_STORAGE_DESC_PARAMS_FIELD

SCHEMA_FILE_URL = (
    "https://raw.githubusercontent.com/opendatadiscovery/"
    "opendatadiscovery-specification/main/specification/extensions/hive.json"
)


class MetadataExtractor:
    __table_excludes = {"Database:", "Owner:", "CreateTime:"}

    def extract_dataset_metadata(self, raw_table_data: List[tuple]) -> Dict[str, Any]:
        return {
            "schema_url": f"{SCHEMA_FILE_URL}#/definitions/HiveDataSetExtension",
            "metadata": self.__extract_all_entries(
                self.__extract_all_data_batches(raw_table_data),
                exclude=self.__table_excludes,
            ),
        }

    def __extract_all_entries(
        self, all_data_batches: Dict[str, Any], exclude: Set[str]
    ) -> Dict[str, Any]:
        return {k: v for k, v in all_data_batches.items() if k not in exclude}

    def __extract_all_data_batches(self, raw_table_data: List[tuple]) -> Dict[str, Any]:

        batch_detailed_table_info = self.__get_metadata_batch(
            raw_table_data,
            HIVE_DETAILED_TABLE_INFO_FIELD,
            HIVE_TABLE_PARAMETERS_FIELD,
            0,
        )
        batch_table_params = self.__get_metadata_batch(
            raw_table_data,
            HIVE_TABLE_PARAMETERS_FIELD,
            HIVE_STORAGE_INFORMATION_FIELD,
            1,
        )
        batch_storage_info = self.__get_metadata_batch(
            raw_table_data,
            HIVE_STORAGE_INFORMATION_FIELD,
            HIVE_STORAGE_DESC_PARAMS_FIELD,
            0,
        )
        batch_storage_desc_params = self.__get_metadata_batch(
            raw_table_data, HIVE_STORAGE_DESC_PARAMS_FIELD, None, 0
        )
        return {
            **batch_detailed_table_info,
            **batch_table_params,
            **batch_storage_info,
            **batch_storage_desc_params,
        }

    def __get_metadata_batch(
        self,
        raw_table_data: List[tuple],
        index_start: str,
        index_finish: str,
        position: int,
    ) -> Dict[str, Any]:
        get_index_start = self.__get_index(raw_table_data, index_start) + 1
        get_index_finish = self.__get_index(raw_table_data, index_finish)

        raw_batch = raw_table_data[get_index_start:get_index_finish]
        clean_batch = {
            self.__stripped(i[position]): self.__stripped(i[position + 1])
            for i in raw_batch
            if i[position]
        }
        return clean_batch

    def __stripped(self, value) -> str:
        return value.strip() if isinstance(value, str) else value

    def __get_index(self, raw_table_data, row_value) -> int:
        try:
            return next(
                raw_table_data.index(i) for i in raw_table_data if i[0] == row_value
            )
        except StopIteration:
            logging.warning(
                f"There was an error during getting index "
                f'of "{row_value}". Returning: -1.'
            )
            return -1
