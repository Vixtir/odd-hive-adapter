## OpenDataDiscovery Hive adapter
* [Requirements](#requirements)
* [Test Hadoop cluster](#test-hadoop-cluster)
* [ENV variables](#env-variables)
* [Dataset structure example](#dataset-structure-example)
* [Metadata structure example](#metadata-structure-example)
* [Fieldlist structure example](#fieldlist-structure-example)

 
## Requirements 
* --extra-index-url https://test.pypi.org/simple/
* future==0.18.2
* PyHive==0.5.0
* sasl==0.2.1
* thrift==0.10.0
* thrift-sasl==0.3.0
* python-dateutil==2.8.1
* six==1.15.0
* flask==1.1.2
* gunicorn===20.0.4
* odd-contract-dev==0.0.22
* more-itertools==8.6.0
* lark-parser==0.11.1

NOTE: is case of troubles with sasl package installation locally, try with ```conda install sasl``` 

## Test Hadoop cluster
Docker-compose file to set up test Hadoop cluster with Hive:
https://github.com/tech4242/docker-hadoop-hive-parquet

Update configuration in hive-site.xml with NOSASL authentication:
```
<property><name> hive.server2.authentication</name><value>NOSASL</value></property>
```

Add HIVE-ODD app to this docker-compose:
```
  hive-odd:
    environment:
      SERVICE_PRECONDITION: "namenode:50070 datanode:50075 hive-metastore-postgresql:5432 resourcemanager:8088 hive-metastore:9083 huedb:5000 hive-server:10000"
    env_file:
      - ./hive-adapter/server/.env
    ports:
      - "8080:8080"
    build: hive-adapter/
    command: flask run
```


## ENV variables
```
HIVE_HOST_NAME=host.docker.internal
HIVE_AUTH=NOSASL
```
NOTE: works for test Hadoop cluster with bounded HIVE-ODD app in docker-compose

## Dataset structure example:
```
 {
    "oddrn": "//hdfs/hive/databases/default/tables/hue__tmp_netflix_titles",
    "name": "hue__tmp_netflix_titles",
    "owner": "//hdfs/hive/owners/root",
    "metadata": [],
    "created_at": "2021-03-22T18:02:11+00:00",
    "dataset": {
        "rows_number": 7789,
        "subtype": "DATASET_TABLE",
 }

```

## Metadata structure example:
```
{
    "schema_url": "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/hive.json#/definitions/HiveDataSetExtension",
    "metadata": {
        "LastAccessTime:": "UNKNOWN",
        "Retention:": "0",
        "Location:": "hdfs://namenode:8020/user/hive/warehouse/hue__tmp_netflix_titles",
        "Table Type:": "MANAGED_TABLE",
        "COLUMN_STATS_ACCURATE": "{\\\"BASIC_STATS\\\":\\\"true\\\",\\\"COLUMN_STATS\\\":{\\\"country\\\":\\\"true\\\",\\\"date_added\\\":\\\"true\\\",\\\"description\\\":\\\"true\\\",\\\"director\\\":\\\"true\\\",\\\"duration\\\":\\\"true\\\",\\\"listed_in\\\":\\\"true\\\",\\\"rating\\\":\\\"true\\\",\\\"release_year\\\":\\\"true\\\",\\\"show_id\\\":\\\"true\\\",\\\"title\\\":\\\"true\\\",\\\"type\\\":\\\"true\\\"}}",
        "numFiles": "1",
        "numRows": "7789",
        "rawDataSize": "2984813",
        "skip.header.line.count": "1",
        "totalSize": "3000491",
        "transient_lastDdlTime": "1616512625",
        "SerDe Library:": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        "InputFormat:": "org.apache.hadoop.mapred.TextInputFormat",
        "OutputFormat:": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "Compressed:": "No",
        "Num Buckets:": "-1",
        "Bucket Columns:": "[]",
        "Sort Columns:": "[]"
    }
}
```
## Fieldlist structure example:
```
"field_list": [   
                {
                    "oddrn": "//hdfs/hive/databases/default/tables/hue__tmp_netflix_titles/columns/director",
                    "name": "director",
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
                            "max_length": 49,
                            "avg_length": -1.0,
                            "nulls_count": 2,
                            "unique_count": 4083
                        }
                    }
                },
                {
                    "oddrn": "//hdfs/hive/databases/default/tables/hue__tmp_netflix_titles/columns/date_added",
                    "name": "date_added",
                    "metadata": [],
                    "type": {
                        "type": "TYPE_DATETIME",
                        "logical_type": "date",
                        "is_nullable": true
                    },
                    "is_key": false,
                    "is_value": false,
                    "stats": {}
                },
                {
                    "oddrn": "//hdfs/hive/databases/default/tables/hue__tmp_netflix_titles/columns/release_year",
                    "name": "release_year",
                    "metadata": [],
                    "type": {
                        "type": "TYPE_NUMBER",
                        "logical_type": "bigint",
                        "is_nullable": true
                    },
                    "is_key": false,
                    "is_value": false,
                    "stats": {
                        "number_stats": {
                            "low_value": 2010.0,
                            "high_value": 2018.0,
                            "nulls_count": 7787,
                            "unique_count": 2
                        }
                    }
                }]
```
