import pandas as pd

def handle_missing_values(data):
    # Replace missing values with "NA"
    data = data.fillna("NA")

    return data

def handle_data_consistency(data):
    # Convert inconsistent date formats to a standard format
    data['date_column'] = pd.to_datetime(data['date_column'], errors='coerce')

    # Convert inconsistent categorical values to lowercase
    data['category_column'] = data['category_column'].str.lower()

    return data

def validate_data_range(data):
    # Check if numerical values are within a reasonable range
    data = data[(data['numeric_column'] >= 0) & (data['numeric_column'] <= 100)]

    # Check if dates are within a specific timeframe
    data = data[data['date_column'].between('2022-01-01', '2022-12-31')]

    # Verify that categorical values match predefined categories
    predefined_categories = ['category1', 'category2', 'category3']
    data = data[data['category_column'].isin(predefined_categories)]

    return data

def validate_data_types(data):
    # Ensure numerical columns contain only numeric values
    data['numeric_column'] = pd.to_numeric(data['numeric_column'], errors='coerce')

    # Ensure date columns are in a valid date format
    data['date_column'] = pd.to_datetime(data['date_column'], errors='coerce')

    # Ensure categorical columns contain only string values
    data['category_column'] = data['category_column'].astype(str)

    return data

def handle_duplicates(data):
    # Remove duplicate rows
    data = data.drop_duplicates()

    return data

# Load the data from a CSV file
data = pd.read_csv('notebook\data\employee_data.csv')

# Handle missing values
data = handle_missing_values(data)

# Handle data consistency
data = handle_data_consistency(data)

# Validate data range
data = validate_data_range(data)

# Validate data types
data = validate_data_types(data)

# Handle duplicates
data = handle_duplicates(data)

# Print the cleaned and validated data
print(data)
