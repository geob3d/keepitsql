from keepitsql.core.insert import Insert
from keepitsql.core.upsert import Upsert


class FromDataframe(Upsert, Insert):
    def __init__(self, target_table, dataframe, target_schema=None):
        """Initializes a new instance of the FromDataframe class.

        Parameters
        ----------
        - target_table: str. The name of the target table for SQL operations.
        - dataframe: DataFrame. The dataframe containing the data to be upserted.
        - target_schema: str, optional. The schema of the target table, if applicable.
        """
        super().__init__(target_table, target_schema, dataframe)


# Test
data = {
    "ItemID": ["ID101", "ID102", "ID103", "ID104", "ID105"],
    "ItemName": ["Laptop", "Desk Chair", "USB-C Cable", "Monitor", "Mouse"],
    "Description": [
        "15-inch laptop with 8GB RAM",
        "Ergonomic office chair",
        "1m USB-C charging cable",
        "24-inch LED monitor",
        "Magic Apple",
    ],
    "Category": ["Electronics", "Furniture", "Electronics", "Electronics", "Acessories"],
    "Quantity": [10, 5, 50, 8, 4],
    "Location": ["Warehouse A", "Warehouse B", "Warehouse A", "Warehouse C", "Warehouse C"],
}

import pandas as pd

df3 = pd.DataFrame(data)
merge_test = FromDataframe("sample", df3, "dd")

# ints = merge_test.merge(source_table='human', match_condition=['ItemId'])
in3 = merge_test.upsert(source_table='human', match_condition=['ItemID'], dbms_output='postgresql')


# merge_test = FromDataframe("sample", df3, "dd")
# merge_test.source_table = 'human'
# merge_test.source_schema= None
# merge_test.match_condition = ['ItemID']
# merge_test.dbms_output = 'mssql'
# merge_test.column_exclusion = None
# print(merge_test.upsert())


# merge_test.insert_on_conflict(source_table='human', match_condition=['ItemID'])

# print(in3)
