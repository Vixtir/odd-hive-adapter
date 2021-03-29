from typing import Any, Dict
from flask import Response
from odd_contract.controllers import ODDController
from odd_contract.encoder import JSONEncoder

from server.adapter import HiveAdapter
from server.config import get_env

class OpenDataDiscoveryAdapterController(ODDController):
    encoder = JSONEncoder()

    def get_data_entities(cls, params: Dict[str, Any]):
        hive_adapter = HiveAdapter(host_name=get_env('HOST_NAME'),
                                   port=get_env('PORT'),
                                   user=get_env('USER'),
                                   password=get_env('PASSWORD'),
                                   auth=get_env('AUTH'))
        datasets = hive_adapter.get_datasets()
        return Response(response=cls.encoder.encode(datasets),
                        content_type='application/json',
                        status=200)
