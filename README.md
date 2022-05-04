# NOTE: Project was moved to [ODD Collector](https://github.com/opendatadiscovery/odd-collector) repository

## OpenDataDiscovery Hive adapter
* [Requirements](#requirements)
* [ENV variables](#env-variables)
* [Dataset structure example](#dataset-structure-example)
* [Metadata structure example](#metadata-structure-example)
* [Field_list structure example](#field_list-structure-example)

 
## Requirements 
* --extra-index-url https://test.pypi.org/simple/
* hive-metastore-client==1.0.7
* python-dateutil==2.8.1
* flask==1.1.2
* flask-compress==1.9.0
* gunicorn===20.0.4
* odd-contract-dev==0.0.29
* more-itertools==8.6.0
* lark-parser==0.11.1
* apscheduler==3.7.0
* pytz==2020.1


## ENV variables
```
HIVE_HOST_NAME
HIVE_PORT
```

## Dataset structure example:
```
"data_source_oddrn": "//hdfs/hive/ip-10-7-6-183.eu-central-1.compute.internal",
    "items": [
        {
            "oddrn": "//hdfs/hive/databases/default/tables/employee",
            "name": "employee",
            "owner": "//hdfs/hive/ip-10-7-6-183.eu-central-1.compute.internal/owners/root",
            "metadata": [{...}],
            "updated_at": "1970-01-01T00:00:00Z",
            "created_at": "2021-04-05T09:11:47Z",
            "dataset": {
                "rows_number": 0,
                "subtype": "DATASET_TABLE",
                "field_list": [{...}, {...}, ... ]
        }, ...

```

## Metadata structure example:
```
"metadata": [{
        "schema_url": "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/hive.json#/definitions/HiveDataSetExtension",
        "metadata": {
            "totalSize": "0",
            "numRows": "0",
            "rawDataSize": "0",
            "COLUMN_STATS_ACCURATE": "{\"BASIC_STATS\":\"true\",\"COLUMN_STATS\":{\"eid\":\"true\",\"name\":\"true\"}}",
            "numFiles": "0",
            "transient_lastDdlTime": "1617804025",
            "bucketing_version": "2"}
    }]
```
## field_list structure example:
```
"field_list": [
                {
                    "oddrn": "//hdfs/hive/databases/test/tables/meteorite/columns/name",
                    "name": "name",
                    "metadata": [],
                    "type": {
                        "type": "TYPE_STRING",
                        "logical_type": "string",
                        "is_nullable": true
                    },
                    "is_key": false,
                    "is_value": false,
                    "stats": {
                        "string_stats": {
                            "max_length": 19,
                            "avg_length": 8.4167,
                            "nulls_count": 0,
                            "unique_count": 24
                        }
                    }
                },
                {
                    "oddrn": "//hdfs/hive/databases/test/tables/meteorite/columns/id",
                    "name": "id",
                    "metadata": [],
                    "type": {
                        "type": "TYPE_INTEGER",
                        "logical_type": "smallint",
                        "is_nullable": true
                    },
                    "is_key": false,
                    "is_value": false,
                    "stats": {
                        "number_stats": {
                            "low_value": 1.0,
                            "high_value": 461.0,
                            "nulls_count": 0,
                            "unique_count": 24
                        }
                    }
                }, ...
             ]
```
