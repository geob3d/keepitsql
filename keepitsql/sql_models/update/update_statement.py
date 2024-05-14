target_table = '{table_name}'
set_column_list = '''t.{column}={column}'''


update_syntax_from_table = '''
UPDATE t
SET {set_column_list}
FROM {target_table} t
;

'''

ig = set_column_list.format(column='dd')
print(ig)
