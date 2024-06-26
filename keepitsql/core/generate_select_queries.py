import os
from datetime import datetime
from typing import (
    Dict,
    List,
)

from sqlalchemy import (
    Engine,
    create_engine,
    inspect,
)

# Database connection string
database_url = "sqlite:////Users/themobilescientist/Documents/projects/archive/keepitsql/test.db"  # Update this to your database URL

# Create an engine
engine = create_engine(database_url)


def format_column(column: Dict) -> str:
    """
    Format the column name based on its data type and nullability.

    Args:
        column (Dict): A dictionary containing column details, including the name and type.

    Returns:
        str: A formatted column name based on its type.
    """
    numeric_types = ['int', 'float', 'money', 'decimal', 'numeric', 'smallint', 'tinyint', 'bigint', 'real']
    if column['type'].__class__.__name__.lower() in numeric_types:
        return f"COALESCE({column['name']}, 0) AS {column['name']}"
    else:
        return column['name']


def export_select_statements(
    engine: Engine,
    output_path: str,
    include_joins: bool = False,
    file_prefix: str = '',
    file_suffix: str = '',
    overwrite: bool = False,
) -> None:
    """
    Generate SQL SELECT statements with optional joins and custom file naming options, handling file overwriting.

    Args:
        engine (Engine): SQLAlchemy Engine connected to the database.
        output_path (str): The directory path where SQL files will be saved.
        include_joins (bool): Whether to include JOIN statements based on foreign keys.
        file_prefix (str): Prefix to add to the filename.
        file_suffix (str): Suffix to add to the filename.
        overwrite (bool): If False, create a new directory to avoid overwriting files.
    """
    # Adjust output path based on overwrite option
    if not overwrite:
        database_name = engine.url.database
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_path, f"{database_name}_{timestamp}")

    # Ensure the output directory exists
    os.makedirs(output_path, exist_ok=True)

    inspector = inspect(engine)
    for schema in inspector.get_schema_names():
        for table_name in inspector.get_table_names(schema=schema):
            columns = inspector.get_columns(table_name, schema=schema)
            # Formatting columns with commas in front
            formatted_columns = [format_column(column) for column in columns]
            column_list = ",\n    ".join(formatted_columns)
            column_list = column_list.replace(", ", ",\n    ", 1)  # Adjust comma placement for the first column

            base_query = (
                f"SELECT \n    {column_list} \nFROM \n    {schema}.{table_name}"
                if schema
                else f"SELECT \n    {column_list} \nFROM \n    {table_name}"
            )

            if include_joins:
                fks = inspector.get_foreign_keys(table_name, schema=schema)
                for fk in fks:
                    referenced_table = fk['referred_table']
                    referenced_schema = fk['referred_schema']
                    join_condition = " AND ".join(
                        f"{table_name}.{local_col} = {referenced_table}.{remote_col}"
                        for local_col, remote_col in zip(fk['constrained_columns'], fk['referred_columns'])
                    )
                    # Formatting join statement
                    base_query += (
                        f"\nJOIN {referenced_table} \n    ON {join_condition}"
                        if not referenced_schema
                        else f"\nJOIN {referenced_schema}.{referenced_table} \n    ON {join_condition}"
                    )

            query = f"{base_query};"
            file_name = f"{file_prefix}{table_name}{file_suffix}.sql"
            file_path = os.path.join(output_path, file_name)
            with open(file_path, 'w') as f:
                f.write(query)
            print(f"Generated SQL script for {table_name} with formatted columns and joins at '{file_path}'.")


# # # Specify the output path
# # output_path = "./sql_scripts"
# # include_joins = True  # Set this to True to include joins based on foreign key relationships
# # file_prefix = 'export_'  # Example prefix, set to '' if none needed
# # file_suffix = '_query'  # Example suffix, set to '' if none needed
# # overwrite = True # Set this to True to overwrite existing files, False to create a new directory


# # # Run the function with the specified options
# # export_select_statements(engine, output_path, include_joins, file_prefix, file_suffix, overwrite)
