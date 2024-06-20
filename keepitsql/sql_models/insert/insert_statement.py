from __future__ import annotations

insert_values = '({ins_value})'

standard_insert = '''
INSERT INTO {table_name} (
    {column_names}
)
VALUES (
    {insert_value_list}
)
'''


insert_select_statment = '''
INSERT INTO {target_table_name} (
 {column_names}
)
SELECT 
 {column_names}
FROM {source_table}

'''

# standard_insert = '''
# INSERT INTO {table_name} (
#  {column_names}
# )
# VALUES \n (\n{insert_value_list}\n)
# '''

# INSERT INTO SPO.computer_systems (
#     ItemID,
#     ItemName,
#     Description,
#     Category,
#     Quantity,
#     Location
# )
# VALUES (
#     :ItemID,
#     :ItemName,
#     :Description,
#     :Category,
#     :Quantity,
#     :Location
# )
