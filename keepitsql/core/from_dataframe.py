from keepitsql.core.insert import Insert
from keepitsql.core.merge import (
    InsertOnConflict,
    Merge,
)


class FromDataframe(Merge, InsertOnConflict, Insert):
    def __init__(self, target_table, dataframe, target_schema) -> None:
        """Initializes a new instance of the FromDataFrame class.

        Parameters
        ----------
        - target_table: str. The name of the target table for SQL operations.
        - target_schema: str, optional. The schema of the target table, if applicable.
        """
        self.dataframe = dataframe
        self.target_table = target_table
        self.target_schema = target_schema


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

int = merge_test.merge(source_table='human', match_condition=['ItemId'])
in3 = merge_test.insert_on_conflict(source_table='human', match_condition=['ItemID'])

print(in3)
# print(int)
