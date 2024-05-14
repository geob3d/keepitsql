from __future__ import annotations

insert_values = '({ins_value})'

standard_insert = '''
INSERT INTO {table_name} (
 {column_names}
)
VALUES \n {insert_value_list}
'''

insert_select_statment = '''
INSERT INTO {target_table_name} (
 {column_names}
)
SELECT 
 {column_names}
FROM {source_table}

'''
