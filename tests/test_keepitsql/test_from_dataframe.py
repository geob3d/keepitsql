# from keepitsql.core.upsert import GenerateMergeStatement
# from keepitsql.core.insert import GenerateInsert
import unittest

import pandas as pd
import polars as pl

from keepitsql.core.from_dataframe import FromDataframe


class TestFromDataframe(unittest.TestCase):
    def setUp(self):
        data = {
            'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva'],
            'Age': [25, 30, 35, 40, 45],
            'City': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'],
            'Salary': [70000, 80000, 90000, 100000, 110000],
        }
        self.test_df = pl.DataFrame(data)
        self.intep = FromDataframe(self.test_df)

    def test_initialization(self):
        self.assertIsInstance(self.intep, FromDataframe)
        self.assertEqual(self.intep.dataframe.shape, self.test_df.shape)

    def test_get_params(self):
        row = self.test_df
        params = self.intep.get_params(row)
        expected_params = {'Name': 'Alice', 'Age': 25, 'City': 'New York', 'Salary': 70000}
        self.assertEqual(params, expected_params)

    def test_dbms_merge_generator(self):
        # Assuming the GenerateMergeStatement class has a method called dbms_merge_generator
        sn = self.intep.dbms_merge_generator("SPO.Users", ['Name'], source_table_name='HIP.users', dbms='sqlite')
        self.assertIsNotNone(sn)

    def test_insert(self):
        # Assuming the GenerateInsert class has a method called insert
        mpop = self.intep.insert("SPO.Users")
        self.assertIsNotNone(mpop)


if __name__ == '__main__':
    unittest.main()
