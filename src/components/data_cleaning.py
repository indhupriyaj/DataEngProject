import os
import sys
import pandas as pd
from src.exception import CustomException
from src.logger import logging
from dataclasses import dataclass
import re
import numpy as np
from sklearn.impute import SimpleImputer
from word2number import w2n

@dataclass
class DataCleaningConfig:
    cleaned_data_path: str = os.path.join('artifacts', 'cleaned_data.csv')

class DataCleaning:
    def __init__(self, raw_data_path):
        self.cleaned_data_path = DataCleaningConfig().cleaned_data_path
        self.raw_data_path = raw_data_path

    def clean_data(self):
        logging.info("Entered the data cleaning method and component")
        try:
            # Read the raw data CSV file into a pandas DataFrame
            self.data = pd.read_csv(self.raw_data_path)
            logging.info("Read the raw data as a DataFrame")

            # Step 2: Handle missing values
            self.data.fillna("NA", inplace=True)

            # Step 3: Remove duplicates
            self.data.drop_duplicates(inplace=True)

            # Step 4: Identify and convert inconsistent string/object columns
            data_types = self.data.apply(pd.api.types.infer_dtype)

            for column, dtype in data_types.items():
                if dtype == 'string':
                    # Pattern matching for email
                    if self.data[column].str.match(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b').all():
                        self.data[column] = self.data[column].astype('category')

                    # Pattern matching for phone number
                    if self.data[column].str.match(r'\b\d{3}-\d{3}-\d{4}\b').all():
                        self.data[column] = self.data[column].astype('category')

                    # Check if the column starts with numbers and contains alphabets
                    if self.data[column].str.match(r'^\d{3}').any() and self.data[column].str.contains(r'[a-zA-Z]').any():
                        # Remove alphabets and convert to int64
                        self.data[column] = self.data[column].str.replace(r'\D', '', regex=True)
                        is_numeric = self.data[column].str.match(r'^\d+$')
                        self.data.loc[is_numeric, column] = self.data.loc[is_numeric, column].astype('int64')

                    # Convert word representations to numeric values
                    try:
                        self.data[column] = self.data[column].apply(lambda x: w2n.word_to_num(x) if isinstance(x, str) else x)
                    except ValueError:
                        logging.warning("Invalid word representation found in column '{}'. Skipping conversion.".format(column))

                elif dtype == 'datetime':
                    # Convert date columns to DDMMYYYY format
                    self.data[column] = self.data[column].dt.strftime('%d%m%Y')

                elif dtype == 'numeric':
                    # Regular expressions
                    self.data[column] = self.data[column].replace(r'[^0-9.]', '', regex=True).astype(int)

                    # Outlier detection
                    # Apply outlier detection techniques (e.g., z-score, IQR, machine learning algorithms)
                    # and handle outliers appropriately

                    # Data imputation
                    #imputer = SimpleImputer(strategy='mean')
                    #self.data[column] = imputer.fit_transform(self.data[[column]])

                # else:
                    # Handle other data types if necessary
                    # ...

            # Create the artifacts directory if it doesn't exist
            os.makedirs(os.path.dirname(self.cleaned_data_path), exist_ok=True)

            # Export the cleaned data to the specified path
            self.data.to_csv(self.cleaned_data_path, index=False, header=True)
            logging.info("Exported cleaned data to CSV")

        except Exception as e:
            raise CustomException(e, sys)


if __name__ == "__main__":
    from src.components.data_ingestion import DataIngestion, DataIngestionConfig
    
    # Path to the raw data file
    raw_data_path = DataIngestionConfig().raw_data_path
    
    obj_cleaning = DataCleaning(raw_data_path)
    obj_cleaning.clean_data()
