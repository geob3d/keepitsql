import unittest

import pandas as pd

from keepitsql.core.from_dataframe import FromDataframe


class TestFromDataframe(unittest.TestCase):
    def setUp(self):
        # Test data
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
            "Category": ["Electronics", "Furniture", "Electronics", "Electronics", "Accessories"],
            "Quantity": [10, 5, 50, 8, 4],
            "Location": ["Warehouse A", "Warehouse B", "Warehouse A", "Warehouse C", "Warehouse C"],
        }
        self.df = pd.DataFrame(data)
        self.target_table = "computer_systems"
        self.target_schema = "SPO"
        self.from_dataframe = FromDataframe(self.target_table, self.df, self.target_schema)

    def test_upsert_merge_with_pk(self):
        # Perform an upsert operation
        upsert_sql = self.from_dataframe.upsert(source_table='human', match_condition=['ItemID'], dbms_output='mssql')
        expected_sql_fragment = "MERGE INTO"
        self.assertIn(expected_sql_fragment, upsert_sql)
        print("Merge SQL Without Primary Key:")
        print(upsert_sql)
        print('-' * 40)

    def test_upsert_merge_without_pk(self):
        # Perform an upsert operation

        upsert_sql = self.from_dataframe.upsert(
            source_table='human', match_condition=['ItemID'], dbms_output='mssql', constraint_columns=['ItemID']
        )
        expected_sql_fragment = "MERGE INTO"
        self.assertIn(expected_sql_fragment, upsert_sql)
        print("Merge SQL With Primary Key:")
        print(upsert_sql)
        print('-' * 40)

    def test_upsert(self):
        # Perform an upsert operation

        upsert_sql = self.from_dataframe.upsert(source_table='human', match_condition=['ItemID'], dbms_output='sqlite')
        expected_sql_fragment = "INSERT INTO"
        self.assertIn(expected_sql_fragment, upsert_sql)
        print("INSERT ON CONFLICT SQL With Primary Key:")
        print(upsert_sql)
        print('-' * 40)

    def test_insert(self):
        # Perform an insert operation (assuming insert method exists in FromDataframe)
        insert_sql = self.from_dataframe.insert()
        expected_sql_fragment = "INSERT INTO"
        self.assertIn(expected_sql_fragment, insert_sql)
        print("Insert SQL:")
        print(insert_sql)
        print('-' * 40)


if __name__ == '__main__':
    unittest.main()
