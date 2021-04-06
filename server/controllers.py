import pytz
import logging
from datetime import datetime
from flask import Response
from typing import List, Tuple, Any, Dict
from odd_contract.controllers import ODDController
from odd_contract.encoder import JSONEncoder
from cache import HiveDataCache


class OpenDataDiscoveryController(ODDController):
    encoder = JSONEncoder()
    empty_cache_response = Response(status=503, headers={'Retry-After': 30})

    def __init__(self,
                 hive_data_cache: HiveDataCache,
                 unit_id: str):
        self.__hive_data_cache = hive_data_cache
        self.__unit_id = unit_id

    def get_data_entities(self, changed_since: Dict[str, Any] = None):
        changed_since = pytz.UTC.localize(datetime.strptime(changed_since['changed_since'], "%Y-%m-%dT%H:%M:%SZ")) \
            if changed_since['changed_since'] is not None \
            else None

        data_entities = self.__hive_data_cache.retrieve_data_entities(changed_since=changed_since)

        if data_entities is None:
            logging.warning('DataEntities cache has never been enriched')
            return self.empty_cache_response
        return self.__build_response(data_entities)

    def __build_response(self, data: Tuple[List, datetime]):
        return Response(
            response=self.encoder.encode({
                'data_source_oddrn': f'//hdfs/hive/{self.__unit_id}',
                'items': data[0]
            }),
            headers={'Last-Modified': data[1].strftime("%a, %d %b %Y %H:%M:%S GMT")},
            content_type='application/json',
            status=200
        )

