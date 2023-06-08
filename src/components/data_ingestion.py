import os
import sys
from src.exception import CustomException
from src.logger import logging
import pandas as pd
from dataclasses import dataclass


@dataclass
class DataIngestionConfig:
    raw_data_path: str = os.path.join('artifacts', "data.csv")

class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        logging.info("Entered the data ingestion method and component")
        try:
            # Prompt user for the input CSV file path
            #csv_file = input("Enter the path of the input CSV file: ")

            # Read the CSV file into a pandas DataFrame
            df = pd.read_csv('notebook\data\employee_data.csv')
            logging.info("Read the dataset as a DataFrame")

            # Create the artifacts directory if it doesn't exist
            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path), exist_ok=True)

            # Export the raw data to the specified path
            df.to_csv(self.ingestion_config.raw_data_path, index=False, header=True)
            logging.info("Exported CSV file and stored it as raw data")

            return self.ingestion_config.raw_data_path

        except Exception as e:
            raise CustomException(e, sys)


if __name__ == "__main__":
    obj = DataIngestion()
    raw_data_path = obj.initiate_data_ingestion()
