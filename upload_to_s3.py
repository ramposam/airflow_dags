import os
import re

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


def upload_folder_to_s3(local_folder, bucket_name, s3_folder):
    """
    Uploads a local folder and its subfolders/files to an S3 bucket.

    :param local_folder: Path to the local folder to upload
    :param bucket_name: Name of the S3 bucket
    :param s3_folder: Folder path in the S3 bucket where files will be uploaded
    """
    s3_client = boto3.client('s3',
                             aws_access_key_id=os.environ.get("aws_access_key_id"),
                             aws_secret_access_key=os.environ.get("aws_secret_access_key")
                             )

    # Walk through all files and subfolders
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            # Local file path
            local_file_path = os.path.join(root, file)

            # Generate the relative path to maintain folder structure in S3
            relative_path = os.path.relpath(local_file_path, local_folder)
            s3_file_path = os.path.join(s3_folder, relative_path).replace("\\", "/")

            try:
                # Upload file to S3
                print(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_file_path}")
                s3_client.upload_file(local_file_path, bucket_name, s3_file_path)
            except (NoCredentialsError, PartialCredentialsError):
                print("Error: AWS credentials not found or incomplete. Please configure them.")
                return
            except Exception as e:
                print(f"Failed to upload {local_file_path}: {e}")


if __name__ == "__main__":
    # Define the local folder, S3 bucket name, and destination folder in S3
    local_folder = r"C:\Users\Asus\PycharmProjects\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports"  # Replace with the path to your folder
    bucket_name = "rposam-devops-airflow"  # Replace with your S3 bucket name
    s3_folder = "datasets/csse_covid_19_daily_reports"  # Replace with the target folder name in S3
    file_name = ".*11-13-2022.*.csv"

    # Call the function
    # upload_folder_to_s3(local_folder, bucket_name, s3_folder)
    s3_client = boto3.client('s3',
                             aws_access_key_id=os.environ.get("aws_access_key_id"),
                             aws_secret_access_key=os.environ.get("aws_secret_access_key")
                             )
    # List to store matching object keys
    matching_objects = []

    # Compile the regex pattern
    pattern = re.compile(file_name)

    # Paginate through objects under the prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_folder):
        # Check if 'Contents' key exists (if no objects, 'Contents' will not be in response)
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # Apply regex filter to object key
                if pattern.search(key):
                    matching_objects.append(key)

    print(matching_objects)
