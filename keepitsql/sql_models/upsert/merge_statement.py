from __future__ import annotations

merge_condition = 'SOURCE.{source_column} = {target_column}'
when_matched_condition = 'TARGET.{target_column} <> SOURCE.{source_column}'
update_list = ' {target_column} = SOURCE.{source_column}'
merge_insert = 'SOURCE.{source_column}'
merge_insert_columns = '{source_column}'

merge_statement = '''
MERGE INTO {target_table} AS TARGET
USING {source_table} AS SOURCE

ON {merge_join_conditions}

WHEN MATCHED AND (
{matched_condition}
)
THEN UPDATE
SET {update_list}

WHEN NOT MATCHED THEN
INSERT(
{insert_columns}
)
VALUES(
{merge_insert_value}
);
'''
