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
