from keepitsql.core.insert import GenerateInsert
from keepitsql.core.upsert import GenerateMergeStatement


class FromDataframe(GenerateMergeStatement, GenerateInsert):
    def __init__(self, dataframe):
        """
        Initializes a new instance of the FromDataframe class.

        Parameters
        ----------
        dataframe : DataFrame
            The DataFrame containing the data that needs to be upserted or inserted into the target table.
        """
        super().__init__(dataframe)

    def get_params(self, row):
        return {col: row[col] for col in self.dataframe.columns}
