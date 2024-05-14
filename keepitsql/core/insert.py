from keepitsql.core.table_properties import (
    format_table_name,
    prepare_column_select_list,
    select_dataframe_column,
)
from keepitsql.sql_models.insert import insert_statement as ist


class Insert:
    def insert(
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
        self.source_dataframe = self.dataframe[column_select] if column_select is not None else self.dataframe

        target_tbl = format_table_name(
            table_name=self.target_table,
            schema_name=self.target_schema,
            temp_table_type=temp_type,
        )

        # Assuming `prepare_sql_columnlist` handles DataFrame and returns a string of column names for SQL
        get_column_header = prepare_column_select_list(
            self.dataframe,
            column_select,
        )

        # Convert DataFrame values to strings, handle None values, and escape single quotes
        formatted_values = self.dataframe.map(
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
