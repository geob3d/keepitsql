from __future__ import annotations

from keepitsql.sql_models import insert_statement as ist
from keepitsql.sql_models import merge_statement as mst


def select_dataframe_column(
    source_dataframe,
    select_list=None,
    output_type: str = 'df',
):
    """Selects columns from the provided dataframe based on a list of column names. The function can return either a modified dataframe or a list of column names, depending on the 'output_type' parameter.

    Parameters
    ----------
    - source_dataframe: DataFrame (Pandas or Polars). The dataframe from which to select columns.
    - select_list: list of str, optional. A list of column names to select from the dataframe. If None, all columns are selected.
    - output_type: str, default 'df'. Determines the type of the return value. If 'df', returns a dataframe with selected columns; if 'list', returns a list of the selected column names.

    Returns
    -------
    - Depending on the value of 'output_type', returns either a dataframe or a list of column names.

    Raises
    ------
    - ValueError: If 'output_type' is not one of the expected values ('df' or 'list').
    """
    # Select specified columns, or all columns if select_list is None
    selected_data = source_dataframe[select_list] if select_list is not None else source_dataframe

    # Return data according to the specified output type
    if output_type == 'list':
        # Convert the columns to a list if 'list' is specified
        if hasattr(selected_data, 'columns'):  # Pandas DataFrame
            return selected_data.columns.tolist()
        elif hasattr(selected_data, 'schema'):  # Polars DataFrame
            return [field.name for field in selected_data.schema]
    elif output_type == 'df':
        # Return the dataframe as is
        return selected_data
    else:
        # Default action if output_type is not recognized or None
        raise ValueError("output_type must be 'df' or 'list'.")


def format_table_name(
    table_name: str,
    temp_table_type: str = None,
    schema_name: str = None,
) -> str:
    """Formats a SQL table name with optional schema and temporary table type prefixes. It can handle both regular and temporary (local or global) SQL table naming conventions.

    Parameters
    ----------
    - table_name: str. The base name of the table.
    - temp_table_type: str, optional. Specifies the type of temporary table ('local' or 'global'). Affects the table name prefix according to MSSQL syntax.
    - schema_name: str, optional. The schema name to prepend to the table name. Useful for databases that support schema names.

    Returns
    -------
    - str: The formatted table name with appropriate prefixes and schema names applied.

    Raises
    ------
    - ValueError: If 'temp_table_type' is provided but is not one of the expected values ('local' or 'global').
    """
    mssql_temp_type = {'local': '#', 'global': '##'}

    # Prepend schema if specified
    schema_prefix = f'{schema_name}.' if schema_name else ''

    # Handling MSSQL temporary tables with optional schema support
    if temp_table_type in mssql_temp_type:
        if temp_table_type not in mssql_temp_type:
            raise ValueError("The MSSQL temp table type must be 'global' or 'local'")
        return f'{schema_prefix}{mssql_temp_type[temp_table_type]}{table_name}'

    # Handling regular table names with an optional schema
    return f'{schema_prefix}{table_name}'


def prepare_column_select_list(dataframe, select_list) -> str:
    """Prepares a comma-separated string of column names for SQL statements. This string is constructed from a list of column names selected from the given dataframe.

    Parameters
    ----------
    - dataframe: DataFrame (Pandas or Polars). The source dataframe from which to select column names.
    - select_list: list of str or None. A list of column names to include. If None, all columns from the dataframe are included.

    Returns
    -------
    - str: A comma-separated string of column names suitable for inclusion in SQL statements.
    """
    col_sel = select_dataframe_column(
        dataframe,
        select_list=select_list,
        output_type='list',
    )
    headers = ',\n '.join(f'{col}' for col in col_sel)
    return headers


class FromDataFrame:
    """A utility class for converting data from a Pandas or Polars dataframe into SQL insert or merge statements.

    Attributes
    ----------
    - source_dataframe: DataFrame or None. The source dataframe from which SQL statements are generated.
    - target_table: str. The name of the target table in the database.
    - target_schema: str or None. The schema of the target table.

    Methods
    -------
    - set_source_dataframe(self, new_dataframe): Sets the source dataframe.
    - sql_insert(self, column_select=None, temp_type=None): Generates an SQL insert statement.
    - sql_merge(self, source_table, join_keys, source_schema=None, column_exclusion=None, temp_type=None): Generates an SQL merge statement.
    """

    def __init__(self, target_table, target_schema) -> None:
        """Initializes a new instance of the FromDataFrame class.

        Parameters
        ----------
        - target_table: str. The name of the target table for SQL operations.
        - target_schema: str, optional. The schema of the target table, if applicable.
        """
        self.source_dataframe = None
        self.target_table = target_table
        self.target_schema = target_schema

        # pass

    def set_df(self, new_dataframe):
        """Sets or updates the source dataframe for the instance.

        Parameters
        ----------
        - new_dataframe: DataFrame (Pandas or Polars). The new source dataframe to be set.

        Returns
        -------
        - self: Enables method chaining by returning the instance.
        """
        self.source_dataframe = new_dataframe
        return self

    def sql_insert(
        self,
        column_select: list = None,
        temp_type: str = None,
    ) -> str:
        """Generates an SQL INSERT statement for inserting data from the source DataFrame into the target table. This method supports selective column insertion and can format the table name for temporary tables. The values from the DataFrame are formatted as strings, with special handling for None values and escaping single quotes.

        Parameters
        ----------
        - column_select (list of str, optional): A list specifying which columns from the source DataFrame should be included in the INSERT statement. If None, all columns are used.
        - temp_type (str, optional): Specifies the type of temporary table. This affects the naming convention used in the SQL statement. For example, 'local' or 'global' temporary tables in MSSQL. If None, a standard table name format is used.

        Returns
        -------
        - str: A complete SQL INSERT statement ready for execution. This statement includes the formatted table name, column names, and values to insert.

        Raises
        ------
        - ValueError: If `column_select` includes column names not present in the source DataFrame.
        - AttributeError: If the method is called before a source DataFrame is set.

        Notes
        -----
        - The method uses the `format_table_name` function to format the target table name based on the `temp_type` and `target_schema`.
        - Column names for the INSERT statement are prepared using the `prepare_column_select_list` function, which handles both Pandas and Polars DataFrames.
        - The source DataFrame values are converted to string format, handling None values appropriately and escaping single quotes to prevent SQL injection or syntax errors.
        - This method should be called after setting the source DataFrame using `set_source_dataframe`.

        Example usage:
        ```python
        from_dataframe = FromDataFrame(target_table="your_table_name", target_schema="your_schema_name")
        from_dataframe.set_source_dataframe(your_dataframe)
        insert_statement = from_dataframe.sql_insert(column_select=['col1', 'col2'], temp_type='local')
        print(insert_statement)
        ```
        """
        self.source_dataframe = (
            self.source_dataframe[column_select] if column_select is not None else self.source_dataframe
        )

        target_tbl = format_table_name(
            table_name=self.target_table,
            schema_name=self.target_schema,
            temp_table_type=temp_type,
        )

        # Assuming `prepare_sql_columnlist` handles DataFrame and returns a string of column names for SQL
        get_column_header = prepare_column_select_list(
            self.source_dataframe,
            column_select,
        )

        # Convert DataFrame values to strings, handle None values, and escape single quotes
        formatted_values = self.source_dataframe.map(
            lambda x: f"'{str(x).replace('s', 'd')}'" if x is not None else 'NULL',
        )

        # Build the values string by concatenating row values
        value_list = ',\n '.join(f"({','.join(row)})" for row in formatted_values.values)

        # Construct the full INSERT statement
        insert_statement = ist.standard_insert.format(
            table_name=target_tbl,
            column_names=get_column_header,
            insert_value_list=value_list,
        )

        return insert_statement

    def sql_merge(
        self,
        source_table: str,
        join_keys: list,
        source_schema: str = None,
        column_exclusion: list = None,
        temp_type: str = None,
    ) -> str:
        target_table = format_table_name(
            table_name=self.target_table,
            schema_name=self.target_schema,
        )
        source_table = format_table_name(
            table_name=source_table,
            schema_name=source_schema,
            temp_table_type=temp_type,
        )

        all_columns = select_dataframe_column(
            source_dataframe=self.source_dataframe,
            output_type='list',
        )

        # Determine columns to include based on the exclusion list
        if column_exclusion is None:
            column_exclusion = []

        # Ensure join keys and excluded columns are not included in the update/insert operations
        column_inclusion = [col for col in all_columns if col not in join_keys and col not in column_exclusion]

        join_conditions = ',\n  '.join(
            mst.merge_condition.format(source_column=col, target_column=col)
            for col in join_keys
            # if col not in join_keys
        )

        matched_condition = ',\n OR '.join(
            mst.when_matched_condition.format(target_column=col, source_column=col) for col in column_inclusion
        )

        merge_update_list = ',\n'.join(
            mst.update_list.format(target_column=col, source_column=col) for col in column_inclusion
        )

        prepare_column_select_list(self.source_dataframe, join_keys)

        merge_insert_values = ',\n'.join(mst.merge_insert.format(source_column=col) for col in column_inclusion)
        merge_insert_columns = ',\n'.join(
            mst.merge_insert_columns.format(source_column=col) for col in column_inclusion
        )

        merge_statement = mst.merge_statement.format(
            target_table=target_table,
            source_table=source_table,
            merge_join_conditions=join_conditions,
            matched_condition=matched_condition,
            update_list=merge_update_list,
            insert_columns=merge_insert_columns,
            merge_insert_value=merge_insert_values,
        )

        return merge_statement


# # # Test for Merge and Insert
# #         "15-inch laptop with 8GB RAM",
# #         "Ergonomic office chair",
# #         "1m USB-C charging cable",
# #         "24-inch LED monitor",


# def some_kwargs(
#     source_table: str,
#     join_keys: list,
# ):
