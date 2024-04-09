import pandas as pd
from src.middleware import error_handler
from ochestrator.ochestrator import load_configs

configs = load_configs()
columns_to_drop_due_nan = configs['columns_to_drop_due_nan']
unnecessary_columns_to_dropped = configs['unnecessary_columns_to_dropped']


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
    cleaned_data = cleaned_data.dropna(subset=columns_to_drop_due_nan, how='any')

    # Remove unnecessary columns
    unnecessary_columns = unnecessary_columns_to_dropped
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
        if data[col].dtype == 'O':
            if col in ['additionalCostsRaw', 'registrationCost', 'deposit']:
                data[col] = data[col].str.replace(r'\u20ac', '€', regex=True)
                data[col] = data[col].str.replace(r'\'', "'", regex=True)
                data[col] = data[col].str.replace(r'\\u2019', "'", regex=True)
                data[col] = data[col].str.replace('-', '€ 00', regex=False)
                data[col] = data[col].str.replace("€ 0'", "€ 0.00'")
                data[col] = data[col].str.replace(r'\b(\d+)\b(?!\.|(\.\d{1,2}))', r'\1.00', regex=True)
            elif col == 'availability':
                data[col] = data[col].str.replace(r"'(\d+)", r"\1", regex=True)
            elif col in ['descriptionTranslated', 'energyLabel']:
                data[col] = data[col].fillna('Unknown')
                data[col] = data[col].replace('', 'Unknown')
                data[col] = data[col].replace('.', 'Unknown')
            else:
                # Apply default transformation if no specific transformation is needed
                data[col] = data[col].str.replace(' /', ',', regex=False)
                data[col] = data[col].str.replace('/', ', ', regex=False)
                data[col] = data[col].str.replace('\"', '', regex=False)
                data[col] = data[col].str.replace('\t', '', regex=False)
                data[col] = data[col].str.strip()

    print("Preprocessing completed successfully.")
    return data
