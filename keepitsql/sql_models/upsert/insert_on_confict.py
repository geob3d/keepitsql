insert_syntax = '{insert_statement}'
update_syntax = '{update_statement}'
update_list = '{column} = EXCLUDED.{column}'


insert_on_conflict = '''
{insert_statment}
ON CONFLICT ({match_condition})
DO UPDATE SET
{update_list}
'''
