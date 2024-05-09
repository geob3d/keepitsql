from __future__ import annotations

create_temp_table_header = {
    'pg': 'CREATE TEMP TABLE {table_name} ',
    'mysql_global': 'CREATE TEMPORARY TABLE {table_name} ',
    'mssql_global': 'CREATE TABLE ##{table_name} ',
    'mssql_local': 'CREATE TABLE #{table_name} ',
    'sqlite': 'CREATE TEMP TABLE {table_name} ',
    'oracle': 'CREATE GLOBAL TEMPORARY TABLE {table_name} ',
}


create_table_header = 'CREATE TABLE {table_name}'


create_column_constraint = '{referred_table_name}_{referred_column_name}_{local_table_name}_{local_column_name}_fk'
create_tbl_column = '{column_name} {column_type}'
create_table = '''{table_header} (\n{column_list} \n{primary_key});'''
