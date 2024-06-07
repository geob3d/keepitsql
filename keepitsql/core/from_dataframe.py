from keepitsql.core.insert import Insert
from keepitsql.core.upsert import Upsert


class FromDataframe(Upsert, Insert):
    def __init__(self, target_table, dataframe, target_schema=None):
        """Initializes a new instance of the FromDataframe class.

        Parameters
        ----------
        - target_table: str. The name of the target table for SQL operations.
        - dataframe: DataFrame. The dataframe contclass FromDataframe(Upsert, Insert):
            def __init__(self, target_table, dataframe, target_schema=None):

                Initializes a new instance of the FromDataframe class, which is designed to perform upsert and insert operations on a SQL table using data from a pandas DataFrame.

                Inherits from:
                Upsert -- A class that implements the logic for updating existing records or inserting new records if they do not exist.
                Insert -- A class that implements the logic for inserting new records into a SQL table.

                Parameters
                ----------
                target_table : str
                    The name of the target table where SQL operations will be performed.

                dataframe : DataFrame
                    The pandas DataFrame containing the data that needs to be upserted or inserted into the target table.



                Methods
                -------
                The class should implement methods inherited from `Upsert` and `Insert` classes for executing SQL operations.
                These methods will utilize `target_table`, `dataframe`, and `target_schema` as part of their operation.

                Examples
                --------
                df_data = pandas.read_csv('path/to/data.csv')
                sql_upserter = FromDataframe('my_table', df_data, 'public')
                sql_upserter.upsert()
        """
        super().__init__(target_table, target_schema, dataframe)


# # Test
# data = {
#     "ItemID": ["ID101", "ID102", "ID103", "ID104", "ID105"],
#     "ItemName": ["Laptop", "Desk Chair", "USB-C Cable", "Monitor", "Mouse"],
#     "Description": [
#         "15-inch laptop with 8GB RAM",
#         "Ergonomic office chair",
#         "1m USB-C charging cable",
#         "24-inch LED monitor",
#         "Magic Apple",
#     ],
#     "Category": ["Electronics", "Furniture", "Electronics", "Electronics", "Acessories"],
#     "Quantity": [10, 5, 50, 8, 4],
#     "Location": ["Warehouse A", "Warehouse B", "Warehouse A", "Warehouse C", "Warehouse C"],
# }

# import pandas as pd

# df3 = pd.DataFrame(data)
# merge_test = FromDataframe("sample", df3, "dd")

# # ints = merge_test.merge(source_table='human', match_condition=['ItemId'])
# in3 = merge_test.upsert(source_table='human', match_condition=['ItemID'], dbms_output='postgresql')


# merge_test = FromDataframe("sample", df3, "dd")
# merge_test.source_table = 'human'
# merge_test.source_schema= None
# merge_test.match_condition = ['ItemID']
# merge_test.dbms_output = 'mssql'
# merge_test.column_exclusion = None
# print(merge_test.upsert())


# merge_test.insert_on_conflict(source_table='human', match_condition=['ItemID'])

# print(in3)
