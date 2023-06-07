import os
import sys
from src.exception import CustomException
from src.logger import logging
import pandas as pd
from dataclasses import dataclass

@dataclass
class DataIngestionConfig:
    cleaned_data_path: str = os.path.join('artifacts', "cleaned_data.csv")
    raw_data_path: str = os.path.join('artifacts', "data.csv")

class DataCleaning:
    def __init__(self, raw_data_path, cleaned_data_path):
        self.raw_data_path = raw_data_path
        self.cleaned_data_path = cleaned_data_path

    def clean_data(self):
        logging.info("Entered the data cleaning method and component")
        try:
            # Read the raw data CSV file into a pandas DataFrame
            df = pd.read_csv(self.raw_data_path)
            logging.info("Read the raw data as a DataFrame")

            # Replace null values with "NA"
            df.fillna("NA", inplace=True)

            # Remove duplicate rows
            df.drop_duplicates(inplace=True)

            # Perform data type checking and correction
            # ...

            # Perform date column validation and correction
            # ...

            # Create the artifacts directory if it doesn't exist
            os.makedirs(os.path.dirname(self.cleaned_data_path), exist_ok=True)

            # Export the cleaned data to the specified path
            df.to_csv(self.cleaned_data_path, index=False, header=True)
            logging.info("Exported cleaned data to CSV")

        except Exception as e:
            raise CustomException(e, sys)


if __name__ == "__main__":
    # Data cleaning
    ingestion_config = DataIngestionConfig()
    obj_cleaning = DataCleaning(ingestion_config.raw_data_path, ingestion_config.cleaned_data_path)
    obj_cleaning.clean_data()
