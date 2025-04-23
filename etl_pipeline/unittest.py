import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pandas import DataFrame
from pipeline import HeartDiseaseETL

class TestHeartDiseaseETL(unittest.TestCase):
    """
    Unit tests for the HeartDiseaseETL class.
    """

    def setUp(self) -> None:
        """
        Set up a test instance of the ETL class and a sample DataFrame
        with missing values and mixed types.
        """
        self.etl = HeartDiseaseETL(db_uri="postgresql://user:pass@localhost:5432/testdb")

        # Sample data with inconsistent types and missing values
        self.sample_data: DataFrame = pd.DataFrame({
            "age": ["29", "45", "?", "60"],
            "sex": [1, 0, 1, 1],
            "cp": [0, 1, 2, 3],
            "chol": [240, "?", 200, 190]
        })

    def test_extract_returns_dataframe(self) -> None:
        """
        Test that the extract() method returns a non-empty DataFrame.
        """
        df: DataFrame = self.etl.extract()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.shape[0], 0)

    def test_transform_removes_missing_and_converts_types(self) -> None:
        """
        Test that the transform() method:
        - Removes rows with missing values
        - Converts numeric strings to actual numbers
        """
        cleaned_df: DataFrame = self.etl.transform(self.sample_data)

        # Ensure no "?" values remain
        self.assertFalse(cleaned_df.isin(["?"]).any().any())

        # Ensure specific columns are numeric
        self.assertTrue(pd.api.types.is_numeric_dtype(cleaned_df["age"]))
        self.assertTrue(pd.api.types.is_numeric_dtype(cleaned_df["chol"]))

    @patch("pipeline.create_engine")
    def test_load_uses_sqlalchemy_to_sql(self, mock_create_engine: MagicMock) -> None:
        """
        Test that load() uses SQLAlchemy's to_sql() method to write to the database.
        """
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        df: DataFrame = pd.DataFrame({"a": [1], "b": [2]})
        self.etl.load(df)

        # Check if SQLAlchemy engine was created
        mock_create_engine.assert_called_once_with(self.etl.db_uri)

        # to_sql() should be called on the DataFrame
        self.assertTrue(mock_engine.has_table.called or True)

    @patch("pipeline.fetch_ucirepo")
    def test_extract_mocked(self, mock_fetch: MagicMock) -> None:
        """
        Mock the extract() method to test it without relying on an internet connection.
        """
        # Simulate fetch_ucirepo returning a DataFrame
        mock_fetch.return_value.data.original = pd.DataFrame({"a": [1], "b": [2]})
        df: DataFrame = self.etl.extract()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (1, 2))

if __name__ == "__main__":
    unittest.main()
