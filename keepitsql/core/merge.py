from keepitsql.core.insert import Insert
from keepitsql.core.table_properties import (
    format_table_name,
    prepare_column_select_list,
    select_dataframe_column,
)
from keepitsql.sql_models.upsert import insert_on_confict as ioc
from keepitsql.sql_models.upsert import merge_statement as mst


class Merge:
    def merge(
        self,
        source_table: str,
        match_condition: list,
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
            source_dataframe=self.dataframe,
            output_type='list',
        )

        # Determine columns to include based on the exclusion list
        if column_exclusion is None:
            column_exclusion = []

        # Ensure join keys and excluded columns are not included in the update/insert operations
        column_inclusion = [col for col in all_columns if col not in match_condition and col not in column_exclusion]

        join_conditions = ',\n  '.join(
            mst.merge_condition.format(source_column=col, target_column=col)
            for col in match_condition
            # if col not in join_keys
        )

        matched_condition = ',\n OR '.join(
            mst.when_matched_condition.format(target_column=col, source_column=col) for col in column_inclusion
        )

        merge_update_list = ',\n'.join(
            mst.update_list.format(target_column=col, source_column=col) for col in column_inclusion
        )

        # prepare_column_select_list(self.dataframe, match_condition)

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


class InsertOnConflict(Insert):
    def insert_on_conflict(
        self,
        source_table: str,
        match_condition: list,
        source_schema: str = None,
        column_exclusion: list = None,
        temp_type: str = None,
    ):
        insert_stmt = self.insert()
        update_list = '\n,'.join

        update_list_col = select_dataframe_column(source_dataframe=self.dataframe, output_type='list')

        match_conditions = ','.join(
            match_condition
            # if col not in join_keys
        )

        update_list = ',\n'.join(
            ioc.update_list.format(column=col)
            for col in update_list_col
            if col.upper() not in list(map(lambda x: x.upper(), match_condition))
        )
        # if col not in join_keys

        on_conflict_statement = ioc.insert_on_conflict.format(
            insert_statment=insert_stmt, match_condition=match_conditions, update_list=update_list
        )

        print(on_conflict_statement)


# class Upsert:
# class FromDataFrame:
#     """A utility class for converting data from a Pandas or Polars dataframe into SQL insert or merge statements.

#     Attributes
#     ----------
#     - dataframe: DataFrame or None. The source dataframe from which SQL statements are generated.
#     - target_table: str. The name of the target table in the database.
#     - target_schema: str or None. The schema of the target table.

#     Methods
#     -------
#     - set_dataframe(self, new_dataframe): Sets the source dataframe.
#     - sql_insert(self, column_select=None, temp_type=None): Generates an SQL insert statement.
#     - sql_merge(self, source_table, join_keys, source_schema=None, column_exclusion=None, temp_type=None): Generates an SQL merge statement.
#     """

#     def __init__(self, target_table, target_schema) -> None:
#         """Initializes a new instance of the FromDataFrame class.

#         Parameters
#         ----------
#         - target_table: str. The name of the target table for SQL operations.
#         - target_schema: str, optional. The schema of the target table, if applicable.
#         """
#         self.dataframe = None
#         self.target_table = target_table
#         self.target_schema = target_schema

#         # pass

#     def set_df(self, new_dataframe):
#         """Sets or updates the source dataframe for the instance.

#         Parameters
#         ----------
#         - new_dataframe: DataFrame (Pandas or Polars). The new source dataframe to be set.

#         Returns
#         -------
#         - self: Enables method chaining by returning the instance.
#         """
#         self.dataframe = new_dataframe
#         return self

#     # def sql_insert(
#     #     self,
#     #     column_select: list = None,
#     #     temp_type: str = None,
#     # ) -> str:
#     #     """Generates an SQL INSERT statement for inserting data from the source DataFrame into the target table. This method supports selective column insertion and can format the table name for temporary tables. The values from the DataFrame are formatted as strings, with special handling for None values and escaping single quotes.

#     #     Parameters
#     #     ----------
#     #     - column_select (list of str, optional): A list specifying which columns from the source DataFrame should be included in the INSERT statement. If None, all columns are used.
#     #     - temp_type (str, optional): Specifies the type of temporary table. This affects the naming convention used in the SQL statement. For example, 'local' or 'global' temporary tables in MSSQL. If None, a standard table name format is used.

#     #     Returns
#     #     -------
#     #     - str: A complete SQL INSERT statement ready for execution. This statement includes the formatted table name, column names, and values to insert.

#     #     Raises
#     #     ------
#     #     - ValueError: If `column_select` includes column names not present in the source DataFrame.
#     #     - AttributeError: If the method is called before a source DataFrame is set.

#     #     Notes
#     #     -----
#     #     - The method uses the `format_table_name` function to format the target table name based on the `temp_type` and `target_schema`.
#     #     - Column names for the INSERT statement are prepared using the `prepare_column_select_list` function, which handles both Pandas and Polars DataFrames.
#     #     - The source DataFrame values are converted to string format, handling None values appropriately and escaping single quotes to prevent SQL injection or syntax errors.
#     #     - This method should be called after setting the source DataFrame using `set_dataframe`.

#     #     Example usage:
#     #     ```python
#     #     from_dataframe = FromDataFrame(target_table="your_table_name", target_schema="your_schema_name")
#     #     from_dataframe.set_dataframe(your_dataframe)
#     #     insert_statement = from_dataframe.sql_insert(column_select=['col1', 'col2'], temp_type='local')
#     #     print(insert_statement)
#     #     ```
#     #     """
#     #     self.dataframe = (
#     #         self.dataframe[column_select] if column_select is not None else self.dataframe
#     #     )

#     #     target_tbl = format_table_name(
#     #         table_name=self.target_table,
#     #         schema_name=self.target_schema,
#     #         temp_table_type=temp_type,
#     #     )

#     #     # Assuming `prepare_sql_columnlist` handles DataFrame and returns a string of column names for SQL
#     #     get_column_header = prepare_column_select_list(
#     #         self.dataframe,
#     #         column_select,
#     #     )

#     #     # Convert DataFrame values to strings, handle None values, and escape single quotes
#     #     formatted_values = self.dataframe.map(
#     #         lambda x: f"'{str(x).replace('s', 'd')}'" if x is not None else 'NULL',
#     #     )

#     #     # Build the values string by concatenating row values
#     #     value_list = ',\n '.join(f"({','.join(row)})" for row in formatted_values.values)

#     #     # Construct the full INSERT statement
#     #     insert_statement = ist.standard_insert.format(
#     #         table_name=target_tbl,
#     #         column_names=get_column_header,
#     #         insert_value_list=value_list,
#     #     )

#     #     return insert_statement

#     def sql_merge(
#         self,
#         source_table: str,
#         join_keys: list,
#         source_schema: str = None,
#         column_exclusion: list = None,
#         temp_type: str = None,
#     ) -> str:
#         target_table = format_table_name(
#             table_name=self.target_table,
#             schema_name=self.target_schema,
#         )
#         source_table = format_table_name(
#             table_name=source_table,
#             schema_name=source_schema,
#             temp_table_type=temp_type,
#         )

#         all_columns = select_dataframe_column(
#             dataframe=self.dataframe,
#             output_type='list',
#         )

#         # Determine columns to include based on the exclusion list
#         if column_exclusion is None:
#             column_exclusion = []

#         # Ensure join keys and excluded columns are not included in the update/insert operations
#         column_inclusion = [col for col in all_columns if col not in join_keys and col not in column_exclusion]

#         join_conditions = ',\n  '.join(
#             mst.merge_condition.format(source_column=col, target_column=col)
#             for col in join_keys
#             # if col not in join_keys
#         )

#         matched_condition = ',\n OR '.join(
#             mst.when_matched_condition.format(target_column=col, source_column=col) for col in column_inclusion
#         )

#         merge_update_list = ',\n'.join(
#             mst.update_list.format(target_column=col, source_column=col) for col in column_inclusion
#         )

#         prepare_column_select_list(self.dataframe, join_keys)

#         merge_insert_values = ',\n'.join(mst.merge_insert.format(source_column=col) for col in column_inclusion)
#         merge_insert_columns = ',\n'.join(
#             mst.merge_insert_columns.format(source_column=col) for col in column_inclusion
#         )

#         merge_statement = mst.merge_statement.format(
#             target_table=target_table,
#             source_table=source_table,
#             merge_join_conditions=join_conditions,
#             matched_condition=matched_condition,
#             update_list=merge_update_list,
#             insert_columns=merge_insert_columns,
#             merge_insert_value=merge_insert_values,
#         )

#         return merge_statement


# # # Test for Merge and Insert
# #         "15-inch laptop with 8GB RAM",
# #         "Ergonomic office chair",
# #         "1m USB-C charging cable",
# #         "24-inch LED monitor",


# def some_kwargs(
#     source_table: str,
#     join_keys: list,
# ):
