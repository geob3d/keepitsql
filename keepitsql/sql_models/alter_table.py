from __future__ import annotations

foreign_key_contraints = '''ALTER TABLE {local_table_name} ADD CONSTRAINT {constraint_name} \nFOREIGN KEY ({local_column}) REFERENCES {reffered_table} ({reffered_column}); '''
