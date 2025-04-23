import pandas as pd
import logging
from ucimlrepo import fetch_ucirepo
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pandas import DataFrame
from typing import List

# ----------------------------
# Logging Setup
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

class HeartDiseaseETL:
    """
    A class that implements an ETL pipeline to extract, transform, and load
    the Heart Disease dataset from the UCI Machine Learning Repository.
    """

    def __init__(self, db_uri: str, table_name: str = "heart_disease", data_id: str = 45) -> None:
        """
        Initialize the ETL pipeline with DB configuration.
        """
        self.db_uri = db_uri
        self.table_name = table_name
        self.data_id = data_id

    def extract(self) -> DataFrame:
        """
        Extract the Heart Disease dataset from UCI repository.

        Returns:
            DataFrame: Raw dataset as a Pandas DataFrame.
        """
        logging.info("Extracting data from UCI repository...")
        heart_disease = fetch_ucirepo(id=self.data_id)
        df = heart_disease.data.original
        logging.info(f"Extracted {df.shape[0]} rows and {df.shape[1]} columns.")
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform the dataset:
        - Remove duplicates
        - Handle missing values
        - Validate numeric types
        - Standardize date formats (if any)

        Args:
            df (DataFrame): Raw dataset.

        Returns:
            DataFrame: Cleaned and validated dataset.
        """
        logging.info("Transforming data...")

        # Remove duplicates
        initial_rows = df.shape[0]
        df = df.drop_duplicates()
        logging.info(f"Removed {initial_rows - df.shape[0]} duplicate rows.")

        # Handle missing values
        df.replace("?", pd.NA, inplace=True)
        df = df.dropna()
        logging.info(f"Removed rows with missing values, remaining: {df.shape[0]} rows.")

        # Convert columns to numeric where possible
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                logging.warning(f"Column '{col}' could not be converted to numeric. Skipping.")

        # Standardize date columns (if any)
        date_cols: List[str] = [col for col in df.columns if 'date' in col.lower()]
        for col in date_cols:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                logging.info(f"Standardized date column '{col}'.")
            except Exception as e:
                logging.warning(f"Could not standardize '{col}' to date format: {e}")

        return df

    def load(self, df: DataFrame) -> None:
        """
        Load the transformed data into PostgreSQL.

        Args:
            df (DataFrame): Cleaned dataset to be loaded.
        """
        logging.info("Loading data into PostgreSQL...")
        try:
            engine: Engine = create_engine(self.db_uri)
            df.to_sql(self.table_name, engine, if_exists="replace", index=False)
            logging.info(f"Data successfully loaded into table '{self.table_name}'.")
        except Exception as e:
            logging.error(f"Failed to load data into PostgreSQL: {e}")

    def run(self) -> None:
        """
        Run the ETL pipeline: Extract → Transform → Load.
        """
        logging.info("Starting ETL pipeline.")
        raw_df = self.extract()
        cleaned_df = self.transform(raw_df)
        self.load(cleaned_df)
        logging.info("ETL pipeline completed successfully.")

# ----------------------------
# Main start
# ----------------------------
if __name__ == "__main__":
    etl = HeartDiseaseETL(db_uri="postgresql://postgres:postgres@db:5432/heartdiseasedb")
    etl.run()
