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
        # Further check the module to differentiate between pandas and polars
        df_type = type(selected_data).__name__
        df_module = selected_data.__class__.__module__

        if df_type == 'DataFrame':
            if 'pandas' in df_module:
                return selected_data.columns.tolist()
            elif 'polars' in df_module:
                return selected_data.columns
        return "Unknown type"
        # # Convert the columns to a list if 'list' is specified
        # if hasattr(selected_data, 'columns'):  # Pandas DataFrame
        #     return selected_data.columns.tolist()
        # elif hasattr(selected_data, 'schema'):  # Polars DataFrame
        #     return [field.name for field in selected_data.schema]

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
