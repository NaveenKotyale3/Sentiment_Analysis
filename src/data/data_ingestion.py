import pandas as pd
from sklearn.model_selection import train_test_split
import yaml
import logging
import os
from src.connections import s3_connection

pd.set_option('future.no_silent_downcasting', True)



def load_params(parmas_path: str) -> dict:
    """Load parameters from a YAML file."""
    try:
        with open(parmas_path, 'r') as file:
            params = yaml.safe_load(file)
        logging.debug(f"Parameters loaded successfully from {parmas_path}.")
        return params
    except FileNotFoundError as e:
        logging.error(f"Parameter file not found: {e}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error loading parameters from {parmas_path}: {e}")
        raise


def load_data(data_url:str)-> pd.DataFrame:
    """Load data from a CSV file."""
    try:
        logging.info(f"Loading data from {data_url}.")
        df = pd.read_csv(data_url)
        logging.info(f"Data loaded successfully from {data_url} with {len(df)} records.")
        return df
    except Exception as e:
        logging.error(f"Error loading data from {data_url}: {e}")
        raise

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data."""
    try:
        # df.drop(columns=['tweet_id'], inplace=True)
        logging.info("pre-processing...")
        final_df = df[df['sentiment'].isin(['positive', 'negative'])]
        final_df['sentiment'] = final_df['sentiment'].replace({'positive': 1, 'negative': 0})
        logging.info('Data preprocessing completed')
        return final_df
    except KeyError as e:
        logging.error('Missing column in the dataframe: %s', e)
        raise
    except Exception as e:
        logging.error('Unexpected error during preprocessing: %s', e)
        raise

def save_data(train_data: pd.DataFrame, test_data: pd.DataFrame,data_path: str):
    """Save the preprocessed data to CSV files."""
    try:
        raw_data_path =os.path.join(data_path, 'raw')
        os.makedirs(raw_data_path, exist_ok=True)

        train_file_path = os.path.join(raw_data_path, 'train.csv')
        test_file_path = os.path.join(raw_data_path, 'test.csv')
        train_data.to_csv(train_file_path, index=False)
        test_data.to_csv(test_file_path, index=False)
        logging.info(f"Data saved successfully to {train_file_path} and {test_file_path}.")
    except Exception as e:
        logging.error(f"Error saving data: {e}")
        raise


def main():
    try:
        paramns = load_params(parmas_path='params.yaml')
        df = load_data(data_url="https://raw.githubusercontent.com/vikashishere/Datasets/refs/heads/main/data.csv")
        # df = s3_connection.s3_operations("","","").fetch_file_from_s3("data.csv")
        final_df = preprocess_data(df)

        test_size = paramns['data_ingestion']['test_size']
        train_data, test_data = train_test_split(final_df, test_size=test_size, random_state=42)
        save_data(train_data, test_data, data_path='./data')
    except Exception as e:
        logging.error(f"Error in data ingestion process: {e}")
        raise


if __name__ == "__main__":
    main()




    
