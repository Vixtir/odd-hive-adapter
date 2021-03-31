from flask import Response
from odd_contract.controllers import ODDController
from odd_contract.encoder import JSONEncoder

from adapter import HiveAdapter


class OpenDataDiscoveryController(ODDController):
    def __init__(self,
                 hive_adapter: HiveAdapter,
                 unit_id: str):
        self._hive_adapter = hive_adapter
        self.__unit_id = unit_id

    encoder = JSONEncoder()

    def get_data_entities(self):
        datasets = self._hive_adapter.get_datasets()
        return Response(
            response=self.encoder.encode(
                {"data_source_oddrn": f"//hdfs/hive/{self.__unit_id}", "items": datasets}
            ),
            content_type="application/json",
            status=200,
        )
