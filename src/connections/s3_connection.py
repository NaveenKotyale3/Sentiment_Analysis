import boto3
import pandas as pd
from io import StringIO
import logging


class s3_operations:

    def __init__(self, bucket_name, aws_access_key_id, aws_secret_access_key, region_name='ap-south-1'):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.bucket_name = bucket_name


    def fetch_file_from_s3(self,file_key):
        """Fetches a file from S3 and returns it as a pandas DataFrame."""
        try:
            logging.info(f"Fetching file {file_key} from bucket {self.bucket_name}.")
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            logging.info(f"File {file_key} fetched successfully from bucket {self.bucket_name}.")
            return df
        
        except Exception as e:
            logging.error(f"Error fetching file {file_key} from bucket {self.bucket_name}: {e}")
            raise


# Example usage
# if __name__ == "__main__":
#     # Replace these with your actual AWS credentials and S3 details
#     BUCKET_NAME = "senti-analysis-ec2"
#     AWS_ACCESS_KEY = ""
#     AWS_SECRET_KEY = ""
#     FILE_KEY = "data.csv"  # Path inside S3 bucket

#     data_ingestion = s3_operations(BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_KEY)
#     df = data_ingestion.fetch_file_from_s3(FILE_KEY)

#     if df is not None:
#         print(f"Data fetched with {len(df)} records..")  # Display first few rows of the fetched DataFrame




