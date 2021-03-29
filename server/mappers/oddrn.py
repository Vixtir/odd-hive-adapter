def get_table_oddrn(database_name: str, table_name: str) -> str:
    return f'//hdfs/hive/databases/{database_name}/tables/{table_name}'


def get_owner_oddrn(owner: str) -> str:
    return f'//hdfs/hive/owners/{owner}'
