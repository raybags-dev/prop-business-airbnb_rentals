import pandas as pd
from src.middleware import error_handler


@error_handler.handle_error
def clean_rentals_data(data):
    """Clean rentals data"""
    if isinstance(data, pd.DataFrame):
        cleaned_data = data.copy()
    else:
        cleaned_data = pd.DataFrame(data)

    # Clean 'rent', 'deposit', 'additionalCostsRaw' columns
    cleaned_data['rent'] = cleaned_data['rent'].str.replace(r'[^\d.]+', '', regex=True)
    cleaned_data['rent'] = pd.to_numeric(cleaned_data['rent'], errors='coerce')
    # Add similar cleaning for 'deposit' and 'additionalCostsRaw' columns

    # Remove rows with missing values
    cleaned_data = cleaned_data.dropna(subset=['rent', 'deposit', 'additionalCostsRaw'], how='any')

    # Remove unnecessary columns
    unnecessary_columns = ['_id', 'crawlStatus', 'crawledAt', 'detailsCrawledAt', 'firstSeenAt', 'lastSeenAt', 'pageDescription', 'pageTitle', 'source']
    cleaned_data = cleaned_data.drop(columns=unnecessary_columns)

    # Remove leading and trailing whitespaces from string columns
    string_columns = cleaned_data.select_dtypes(include='object').columns
    cleaned_data[string_columns] = cleaned_data[string_columns].apply(lambda x: x.str.strip())

    # Preprocess data
    cleaned_data = preprocess_data(cleaned_data)

    return cleaned_data


@error_handler.handle_error
def clean_airbnb_data(data):
    """Clean airbnb data"""
    if isinstance(data, pd.DataFrame):
        # Replace NaN values with empty string
        cleaned_data = data.fillna("")
        return cleaned_data
    else:
        # Convert data to DataFrame and replace NaN values
        cleaned_data = pd.DataFrame(data).fillna("")
        cleaned = preprocess_data(cleaned_data)

    return cleaned


@error_handler.handle_error
def preprocess_data(data):
    for col in data.columns:
        # Check if the column is of object (string) type
        if data[col].dtype == 'O':
            # Apply string cleaning operations to each string column
            data[col] = data[col].str.replace('\\u2019', "'") \
                .str.replace('\u20ac ', '€') \
                .str.replace(' /', ',') \
                .str.replace('/', ', ') \
                .str.replace('\\u20ac ', '€') \
                .str.replace('\"', '') \
                .str.replace('\t', '') \
                .str.replace("-'", "-") \
                .str.replace('-', '€0') \
                .str.strip()

    print("Preprocessing completed successfully.")
    return data


