from __future__ import annotations

import subprocess

# Replace 'your_database_url' with the actual connection string to your database
database_url = 'sqlite:///test.db'

# Replace 'your_table_name' with the actual name of your table (optional)
table_name = 'a'

# Use subprocess to call sqlacodegen with the database URL
command = f'sqlacodegen {database_url} --tables {table_name}' if table_name else f'sqlacodegen {database_url}'

# Execute the command and capture the output
output = subprocess.check_output(command, shell=True, text=True)

# Print the DDL statements
print(output)
