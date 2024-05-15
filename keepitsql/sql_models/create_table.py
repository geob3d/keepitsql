from __future__ import annotations

create_temp_table_headers = {
    "oracle": "CREATE GLOBAL TEMPORARY TABLE {table_name} ",
    "mssql": "CREATE TABLE ##{table_name} ",  # Global temporary table
    "mssql_local": "CREATE TABLE #{table_name} ",  # Local temporary table
    "db2": "",  # DB2 specific statement not provided
    "postgresql": "CREATE TEMP TABLE {table_name} ",
    "mysql": "CREATE TEMPORARY TABLE {table_name} ",
    "sqlite": "CREATE TEMP TABLE {table_name} ",
    "teradata": "",  # Teradata specific statement not provided
    "hana": "",  # SAP HANA specific statement not provided
    "snowflake": "",  # Snowflake specific statement not provided
    "redshift": "CREATE TEMP TABLE {table_name} ",
    "bigquery": "",  # BigQuery specific statement not provided
}


create_table_header = 'CREATE TABLE {table_name}'


create_column_constraint = '{referred_table_name}_{referred_column_name}_{local_table_name}_{local_column_name}_fk'
create_tbl_column = '{column_name} {column_type}'
create_table = '''{table_header} (\n{column_list} \n{primary_key});'''
