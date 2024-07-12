import re

from data_engineer_utils import (
    get_dbms_by_py_driver,
    get_upsert_type_by_dbms,
)
from sqlalchemy.sql.elements import quoted_name

from keepitsql.core.insert import GenerateInsert
from keepitsql.core.table_properties import (
    format_table_name,
    prepare_column_select_list,
    select_dataframe_column,
)
from keepitsql.sql_models.upsert import insert_on_confict as ioc
from keepitsql.sql_models.upsert import merge_statement as mst

# Add to keep it ssql


def quote_identifier(identifier: str) -> str:
    """
    Quote an SQL identifier to prevent SQL injection.

    Args:
        identifier (str): The identifier to quote.

    Returns:
        str: The quoted identifier.
    """
    return quoted_name(identifier, quote=True)


def parse_table_name(qualified_name):
    # Define regex patterns
    simple_pattern = re.compile(r'^(\w+)\.(\w+)$')
    quoted_pattern = re.compile(r'^"(\w+)"\."(\w+)"$')
    fully_qualified_pattern = re.compile(r'^(\w+)\.(\w+)\.(\w+)$')

    if fully_qualified_pattern.match(qualified_name):
        raise ValueError("Fully qualified table name with database name is not allowed.")
    elif simple_pattern.match(qualified_name):
        schema_name, table_name = simple_pattern.match(qualified_name).groups()
        return schema_name, table_name
    elif quoted_pattern.match(qualified_name):
        schema_name, table_name = quoted_pattern.match(qualified_name).groups()
        return schema_name, table_name
    elif '.' not in qualified_name:
        return None, qualified_name
    else:
        raise ValueError("Invalid table name format.")


class GenerateMergeStatement:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def generate_merge_statement(
        self,
        # dataframe: any,
        table_name: str,
        match_condition: list,
        constraint_columns: list = None,
        source_table_name: str = None,
        **kwargs,
    ) -> str:
        """
        Creates a SQL merge statement to merge data from the source table into the target table.

        Args:
            source_table (str): The name of the source table.
            match_condition (list): The list of columns to be used as match conditions.
            source_schema (str, optional): The schema of the source table. Defaults to None.
            constraint_columns (list, optional): The list of columns that are used as constraints,
                                                such as primary keys or auto-update columns,
                                                which should not be inserted. Defaults to None.
            temp_type (str, optional): The type of temporary table to be used. Defaults to None.

        Returns:
            str: The generated SQL merge statement.
        """

        target_table, targe_schema = parse_table_name(table_name)

        all_columns = self.dataframe.columns

        if constraint_columns is None:
            constraint_columns = []

        for item in match_condition:
            if item not in all_columns:
                raise ValueError(f"Value {item} from match condition is not in dataframe.")

        for item in constraint_columns:
            if item not in all_columns:
                raise ValueError(f"Value {item} from match condition is not in dataframe.")

        join_conditions = ' AND\n'.join(
            mst.merge_condition.format(source_column=quote_identifier(col), target_column=quote_identifier(col))
            for col in match_condition
        )
        matched_condition = ' OR\n'.join(
            mst.when_matched_condition.format(target_column=quote_identifier(col), source_column=quote_identifier(col))
            for col in all_columns
            if col not in match_condition
        )

        merge_update_list = ',\n'.join(
            mst.update_list.format(target_column=quote_identifier(col), source_column=quote_identifier(col))
            for col in all_columns
            if col not in match_condition
        )

        merge_insert_values = ',\n'.join(
            mst.merge_insert.format(source_column=quote_identifier(col))
            for col in all_columns
            if col not in constraint_columns
        )
        merge_insert_columns = ',\n'.join(
            mst.merge_insert_columns.format(source_column=quote_identifier(col))
            for col in all_columns
            if col not in constraint_columns
        )

        source_table_name = table_name if source_table_name is None else source_table_name

        merge_statement = mst.merge_statement.format(
            target_table=table_name,
            source_table=source_table_name,
            merge_join_conditions=join_conditions,
            matched_condition=matched_condition,
            update_list=merge_update_list,
            insert_columns=merge_insert_columns,
            merge_insert_value=merge_insert_values,
        )
        return merge_statement

    def generate_insert_on_conflict(
        # dataframe: any,
        self,
        table_name: str,
        match_condition: list,
        constraint_columns: list = None,
        source_table_name: str = None,
        is_sqlite: str = 'N',
        **kwargs
        # source_table: str,
        # match_condition: list,
        # source_schema: str = None,
        # constraint_columns: list = None,
        # temp_type: str = None,
    ):
        """
        Creates a SQL insert statement with conflict resolution to insert data from the source table into the target table.

        Args:
            source_table (str): The name of the source table.
            match_condition (list): The list of columns to be used as match conditions.
            source_schema (str, optional): The schema of the source table. Defaults to None.
            column_exclusion (list, optional): The list of columns to be excluded from the insert. Defaults to None.
            temp_type (str, optional): The type of temporary table to be used. Defaults to None.
        """

        for item in match_condition:
            if item not in self.dataframe.columns:
                raise ValueError(f"Value {item} from list1 is not in list2.")

        init_insert = GenerateInsert(self.dataframe)
        insert_stmt = self.insert(table_name, source_table=source_table_name)

        update_list = '\n,'.join

        update_list_col = select_dataframe_column(source_dataframe=self.dataframe, output_type='list')

        match_conditions = ','.join(match_condition)

        update_list = ',\n'.join(
            ioc.update_list.format(column=col)
            for col in update_list_col
            if col.upper() not in list(map(lambda x: x.upper(), match_condition))
        )
        if is_sqlite == "Y" and source_table_name != None:
            on_conflict_statement = ioc.insert_on_conflict_sqlite.format(
                insert_statment=insert_stmt, match_condition=match_conditions, update_list=update_list
            )
        else:
            on_conflict_statement = ioc.insert_on_conflict.format(
                insert_statment=insert_stmt, match_condition=match_conditions, update_list=update_list
            )

        return on_conflict_statement

    def dbms_merge_generator(
        self,
        table_name: str,
        match_condition: list,
        dbms: str,
        constraint_columns: list = None,
        source_table_name: str = None,
        **kwargs,
    ):
        if get_upsert_type_by_dbms(dbms) == 'MERGE':
            return self.generate_merge_statement(
                table_name=table_name,
                match_condition=match_condition,
                constraint_columns=constraint_columns,
                source_table_name=source_table_name,
            )

        else:
            sqllite_flag = 'Y' if dbms == 'sqlite' else 'N'
            return self.generate_insert_on_conflict(
                table_name=table_name,
                match_condition=match_condition,
                constraint_columns=constraint_columns,
                source_table_name=source_table_name,
                is_sqlite=sqllite_flag,
            )

    # if get_upsert_type_by_dbms(dbms_output) == 'MERGE':
    #     upsert_statment = self.__merge_instance.merge(
    #         source_table=source_table,
    #         match_condition=match_condition,
    #         source_schema=source_schema,
    #         constraint_columns=constraint_columns,
    #         temp_type=temp_type,
    #     )
    # else:
    #     upsert_statment = self.__insert_instance.insert_on_conflict(
    #         source_table=source_table,
    #         match_condition=match_condition,
    #         source_schema=source_schema,
    #         constraint_columns=constraint_columns,
    #         temp_type=temp_type,
    #     )


# class UpsertDataFrame:
#     def __init__(self,dataframe, target_table, target_schema,):
#         """
#         Initializes the Upsert class with the target table, target schema, and dataframe.

#         Args:
#             target_table (str): The name of the target table.
#             target_schema (str): The schema of the target table.
#             dataframe (DataFrame): The dataframe containing the data to be upserted.
#         """
#         self.target_table = target_table
#         self.target_schema = target_schema
#         self.dataframe = dataframe


#     def quote_identifier(self, identifier: str) -> str:
#         """
#         Quote an SQL identifier to prevent SQL injection.

#         Args:
#             identifier (str): The identifier to quote.

#         Returns:
#             str: The quoted identifier.
#         """
#         return quoted_name(identifier, quote=True)

#     def generate_merge_statement(
#         self,
#         source_table: str,
#         match_condition: list,
#         source_schema: str = None,
#         constraint_columns: list = None,
#         temp_type: str = None,
#     ) -> str
#         """
#         Creates a SQL merge statement to merge data from the source table into the target table.

#         Args:
#             source_table (str): The name of the source table.
#             match_condition (list): The list of columns to be used as match conditions.
#             source_schema (str, optional): The schema of the source table. Defaults to None.
#             constraint_columns (list, optional): The list of columns that are used as constraints,
#                                                 such as primary keys or auto-update columns,
#                                                 which should not be inserted. Defaults to None.
#             temp_type (str, optional): The type of temporary table to be used. Defaults to None.

#         Returns:
#             str: The generated SQL merge statement.
#         """
#         target_table = format_table_name(
#             table_name=self.target_table,
#             schema_name=self.target_schema,
#         )
#         source_table = format_table_name(
#             table_name=source_table,
#             schema_name=source_schema,
#             temp_table_type=temp_type,
#         )

#         all_columns = list(self.dataframe.columns)

#         if constraint_columns is None:
#             constraint_columns = []

#         join_conditions = ' AND '.join(
#             mst.merge_condition.format(
#                 source_column=self.quote_identifier(col), target_column=self.quote_identifier(col)
#             )
#             for col in match_condition
#         )

#         matched_condition = ' OR '.join(
#             mst.when_matched_condition.format(
#                 target_column=self.quote_identifier(col), source_column=self.quote_identifier(col)
#             )
#             for col in all_columns
#             if col not in match_condition
#         )

#         merge_update_list = ', '.join(
#             mst.update_list.format(target_column=self.quote_identifier(col), source_column=self.quote_identifier(col))
#             for col in all_columns
#             if col not in match_condition
#         )

#         merge_insert_values = ', '.join(
#             mst.merge_insert.format(source_column=self.quote_identifier(col))
#             for col in all_columns
#             if col not in constraint_columns
#         )
#         merge_insert_columns = ', '.join(
#             mst.merge_insert_columns.format(source_column=self.quote_identifier(col))
#             for col in all_columns
#             if col not in constraint_columns
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


#     def generate_insert_on_conflict(
#         self,
#         source_table: str,
#         match_condition: list,
#         source_schema: str = None,
#         constraint_columns: list = None,
#         temp_type: str = None,
#     ):
#         """
#         Creates a SQL insert statement with conflict resolution to insert data from the source table into the target table.

#         Args:
#             source_table (str): The name of the source table.
#             match_condition (list): The list of columns to be used as match conditions.
#             source_schema (str, optional): The schema of the source table. Defaults to None.
#             column_exclusion (list, optional): The list of columns to be excluded from the insert. Defaults to None.
#             temp_type (str, optional): The type of temporary table to be used. Defaults to None.
#         """
#         insert_stmt = self.insert()
#         update_list = '\n,'.join

#         update_list_col = select_dataframe_column(source_dataframe=self.dataframe, output_type='list')

#         match_conditions = ','.join(match_condition)

#         update_list = ',\n'.join(
#             ioc.update_list.format(column=col)
#             for col in update_list_col
#             if col.upper() not in list(map(lambda x: x.upper(), match_condition))
#         )

#         on_conflict_statement = ioc.insert_on_conflict.format(
#             insert_statment=insert_stmt, match_condition=match_conditions, update_list=update_list
#         )

#         return on_conflict_statement


# class Merge:
#     def __init__(self, target_table: str, target_schema: str = None, dataframe=None):
#         self.target_table = target_table
#         self.target_schema = target_schema
#         self.dataframe = dataframe

#     def quote_identifier(self, identifier: str) -> str:
#         """
#         Quote an SQL identifier to prevent SQL injection.

#         Args:
#             identifier (str): The identifier to quote.

#         Returns:
#             str: The quoted identifier.
#         """
#         return quoted_name(identifier, quote=True)

#     def merge(
#         self,
#         source_table: str,
#         match_condition: list,
#         source_schema: str = None,
#         constraint_columns: list = None,
#         temp_type: str = None,
#     ) -> str:
#         """
#         Creates a SQL merge statement to merge data from the source table into the target table.

#         Args:
#             source_table (str): The name of the source table.
#             match_condition (list): The list of columns to be used as match conditions.
#             source_schema (str, optional): The schema of the source table. Defaults to None.
#             constraint_columns (list, optional): The list of columns that are used as constraints,
#                                                 such as primary keys or auto-update columns,
#                                                 which should not be inserted. Defaults to None.
#             temp_type (str, optional): The type of temporary table to be used. Defaults to None.

#         Returns:
#             str: The generated SQL merge statement.
#         """
#         target_table = format_table_name(
#             table_name=self.target_table,
#             schema_name=self.target_schema,
#         )
#         source_table = format_table_name(
#             table_name=source_table,
#             schema_name=source_schema,
#             temp_table_type=temp_type,
#         )

#         all_columns = list(self.dataframe.columns)

#         if constraint_columns is None:
#             constraint_columns = []

#         join_conditions = ' AND '.join(
#             mst.merge_condition.format(
#                 source_column=self.quote_identifier(col), target_column=self.quote_identifier(col)
#             )
#             for col in match_condition
#         )

#         matched_condition = ' OR '.join(
#             mst.when_matched_condition.format(
#                 target_column=self.quote_identifier(col), source_column=self.quote_identifier(col)
#             )
#             for col in all_columns
#             if col not in match_condition
#         )

#         merge_update_list = ', '.join(
#             mst.update_list.format(target_column=self.quote_identifier(col), source_column=self.quote_identifier(col))
#             for col in all_columns
#             if col not in match_condition
#         )

#         merge_insert_values = ', '.join(
#             mst.merge_insert.format(source_column=self.quote_identifier(col))
#             for col in all_columns
#             if col not in constraint_columns
#         )
#         merge_insert_columns = ', '.join(
#             mst.merge_insert_columns.format(source_column=self.quote_identifier(col))
#             for col in all_columns
#             if col not in constraint_columns
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


# '______'

# # class Merge:
# #     """
# #     Class to handle the merging of data from a source table into a target table using a match condition.

# #     Attributes:
# #         target_table (str): The name of the target table.
# #         target_schema (str): The schema of the target table.
# #         dataframe (DataFrame): The dataframe containing the data to be merged.
# #     """

# #     def __init__(self, target_table, target_schema, dataframe):
# #         """
# #         Initializes the Merge class with the target table, target schema, and dataframe.

# #         Args:
# #             target_table (str): The name of the target table.
# #             target_schema (str): The schema of the target table.
# #             dataframe (DataFrame): The dataframe containing the data to be merged.
# #         """
# #         self.target_table = target_table
# #         self.target_schema = target_schema
# #         self.dataframe = dataframe

# #     def merge(
# #         self,
# #         source_table: str,
# #         match_condition: list,
# #         source_schema: str = None,
# #         constraint_columns: list = None,
# #         temp_type: str = None,
# #     ) -> str:
# #         """
# #         Creates a SQL merge statement to merge data from the source table into the target table.

# #         Args:
# #             source_table (str): The name of the source table.
# #             match_condition (list): The list of columns to be used as match conditions.
# #             source_schema (str, optional): The schema of the source table. Defaults to None.
# #             constraint_columns (list, optional): The list of columns that are used as constraints,
# #                                                 such as primary keys or auto-update columns,
# #                                                 which should not be inserted. Defaults to None.
# #             temp_type (str, optional): The type of temporary table to be used. Defaults to None.

# #         Returns:
# #             str: The generated SQL merge statement.
# #         """
# #         target_table = format_table_name(
# #             table_name=self.target_table,
# #             schema_name=self.target_schema,
# #         )
# #         source_table = format_table_name(
# #             table_name=source_table,
# #             schema_name=source_schema,
# #             temp_table_type=temp_type,
# #         )

# #         all_columns = list(self.dataframe.columns)

# #         if constraint_columns is None:
# #             constraint_columns = []

# #         # column_inclusion = [col for col in all_columns if col not in match_condition and col not in column_exclusion]

# #         join_conditions = 'AND \n  '.join(
# #             mst.merge_condition.format(source_column=col, target_column=col) for col in match_condition
# #         )

# #         matched_condition = '\n OR '.join(
# #             mst.when_matched_condition.format(target_column=col, source_column=col)
# #             for col in all_columns
# #             if col not in match_condition
# #         )

# #         merge_update_list = ',\n'.join(
# #             mst.update_list.format(target_column=col, source_column=col)
# #             for col in all_columns
# #             if col not in match_condition
# #         )

# #         merge_insert_values = ',\n'.join(
# #             mst.merge_insert.format(source_column=col) for col in all_columns if col not in constraint_columns
# #         )
# #         merge_insert_columns = ',\n'.join(
# #             mst.merge_insert_columns.format(source_column=col) for col in all_columns if col not in constraint_columns
# #         )

# #         merge_statement = mst.merge_statement.format(
# #             target_table=target_table,
# #             source_table=source_table,
# #             merge_join_conditions=join_conditions,
# #             matched_condition=matched_condition,
# #             update_list=merge_update_list,
# #             insert_columns=merge_insert_columns,
# #             merge_insert_value=merge_insert_values,
# #         )

# #         return merge_statement


# class InsertOnConflict(Insert):
#     """
#     Class to handle inserting data into a target table with conflict resolution.

#     Attributes:
#         target_table (str): The name of the target table.
#         target_schema (str): The schema of the target table.
#         dataframe (DataFrame): The dataframe containing the data to be inserted.
#     """

#     def __init__(self, target_table, target_schema, dataframe):
#         """
#         Initializes the InsertOnConflict class with the target table, target schema, and dataframe.

#         Args:
#             target_table (str): The name of the target table.
#             target_schema (str): The schema of the target table.
#             dataframe (DataFrame): The dataframe containing the data to be inserted.
#         """
#         self.target_table = target_table
#         self.target_schema = target_schema
#         self.dataframe = dataframe

#     def insert_on_conflict(
#         self,
#         source_table: str,
#         match_condition: list,
#         source_schema: str = None,
#         constraint_columns: list = None,
#         temp_type: str = None,
#     ):
#         """
#         Creates a SQL insert statement with conflict resolution to insert data from the source table into the target table.

#         Args:
#             source_table (str): The name of the source table.
#             match_condition (list): The list of columns to be used as match conditions.
#             source_schema (str, optional): The schema of the source table. Defaults to None.
#             column_exclusion (list, optional): The list of columns to be excluded from the insert. Defaults to None.
#             temp_type (str, optional): The type of temporary table to be used. Defaults to None.
#         """
#         insert_stmt = self.insert()
#         update_list = '\n,'.join

#         update_list_col = select_dataframe_column(source_dataframe=self.dataframe, output_type='list')

#         match_conditions = ','.join(match_condition)

#         update_list = ',\n'.join(
#             ioc.update_list.format(column=col)
#             for col in update_list_col
#             if col.upper() not in list(map(lambda x: x.upper(), match_condition))
#         )

#         on_conflict_statement = ioc.insert_on_conflict.format(
#             insert_statment=insert_stmt, match_condition=match_conditions, update_list=update_list
#         )

#         return on_conflict_statement


# class Upsert:
#     """
#     Class to handle the upserting (insert or update) of data into a target table.

#     Attributes:
#         target_table (str): The name of the target table.
#         target_schema (str): The schema of the target table.
#         dataframe (DataFrame): The dataframe containing the data to be upserted.
#     """

#     def __init__(self, target_table, target_schema, dataframe):
#         """
#         Initializes the Upsert class with the target table, target schema, and dataframe.

#         Args:
#             target_table (str): The name of the target table.
#             target_schema (str): The schema of the target table.
#             dataframe (DataFrame): The dataframe containing the data to be upserted.
#         """
#         self.target_table = target_table
#         self.target_schema = target_schema
#         self.dataframe = dataframe
#         self.__merge_instance = Merge(target_table, target_schema, dataframe)
#         self.__insert_instance = InsertOnConflict(target_table, target_schema, dataframe)

#     def upsert(
#         self,
#         source_table: str,
#         match_condition: list,
#         source_schema: str = None,
#         constraint_columns: list = None,
#         temp_type: str = None,
#         dbms_output=None,
#     ) -> str:
#         """
#         Creates an upsert statement based on the DBMS type. If the DBMS supports MERGE, it creates a merge statement.
#         Otherwise, it creates an insert-on-conflict statement.

#         Args:
#             source_table (str): The name of the source table.
#             match_condition (list): The list of columns to be used as match conditions.
#             source_schema (str, optional): The schema of the source table. Defaults to None.
#             constraint_columns (list, optional): The list of columns that are used as constraints, such as primary keys or
#                                                 auto-update columns, which should not be inserted. This is applied to
#                                                 database systems that use the MERGE statement. Defaults to None.
#             temp_type (str, optional): The type of temporary table to be used. Defaults to None.
#             dbms_output (str, optional): The DBMS type (e.g., 'mssql', 'postgresql'). Defaults to None.

#         Returns:
#             str: The generated upsert statement.
#         """

#         if get_upsert_type_by_dbms(dbms_output) == 'MERGE':
#             upsert_statment = self.__merge_instance.merge(
#                 source_table=source_table,
#                 match_condition=match_condition,
#                 source_schema=source_schema,
#                 constraint_columns=constraint_columns,
#                 temp_type=temp_type,
#             )
#         else:
#             upsert_statment = self.__insert_instance.insert_on_conflict(
#                 source_table=source_table,
#                 match_condition=match_condition,
#                 source_schema=source_schema,
#                 constraint_columns=constraint_columns,
#                 temp_type=temp_type,
#             )
#         return upsert_statment
