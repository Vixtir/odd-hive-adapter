from datetime import datetime
from typing import List, Union, Iterable, Tuple
from odd_contract.models import DataEntity
HiveDataCacheEntry = Tuple[List[DataEntity], datetime]


class HiveDataCache:

    __DATA_ENTITIES: HiveDataCacheEntry = None

    def cache_data_entities(self,
                            datasets: Iterable[DataEntity],
                            updated_at: datetime = datetime.utcnow()):
        self.__DATA_ENTITIES = list(datasets), updated_at

    def retrieve_data_entities(self, changed_since: datetime = None) -> Union[HiveDataCacheEntry, None]:

        if not self.__DATA_ENTITIES:
            return None

        data_entities_filtered = [
            de
            for de in self.__DATA_ENTITIES[0]
            if de.updated_at is None or de.updated_at >= changed_since
        ] if changed_since else self.__DATA_ENTITIES[0]

        return data_entities_filtered, self.__DATA_ENTITIES[1]
