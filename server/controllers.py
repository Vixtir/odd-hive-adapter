from flask import Response
from odd_contract.controllers import ODDController
from odd_contract.encoder import JSONEncoder

from adapter import HiveAdapter
from config import get_env


class OpenDataDiscoveryController(ODDController):
    def __init__(self, hive_adapter: HiveAdapter):
        self._hive_adapter = hive_adapter

    encoder = JSONEncoder()

    def get_data_entities(self):
        datasets = self._hive_adapter.get_datasets()
        host_name = get_env("HOST_NAME")
        return Response(
            response=self.encoder.encode(
                {"data_source_oddrn": f"//hdfs/hive/{host_name}", "items": datasets}
            ),
            content_type="application/json",
            status=200,
        )
