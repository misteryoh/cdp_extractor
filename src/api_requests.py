import requests
import urllib3
import logging
import boto3
from botocore.exceptions import ClientError

def lambda_api_call(path):

    urllib3.disable_warnings()

    ## path: url of and API endpoint
    ## returns: dictionary

    requests.warnings = False

    # Open the Requests Session
    session = requests.Session()

    # GET request from the opened session
    try:
        response = session.get(path, verify=False)
    except:
        return False

    # Check if status_code == 200 to return the response
    if response.status_code == 200:
        return response.json()

def upload_file(request_json, bucket_name, folder_name, object_name):
    """Upload a file to an S3 bucket

    :param request_json: Pandas Dataframe to upload
    :param folder: Bucket to upload to
    :param bucket_name: Folder to upload to
    :param object_name: S3 object name
    :return: True if file was uploaded, else False
    """

    # Upload the file
    # Create a session using the specified configuration file
    session = boto3.Session(profile_name='default')
    s3_client = session.client('s3')

    try:
        # response = s3_client.upload_file(file_name, bucket, object_name)

        # Convert the response to JSON and upload to S3
        s3_object = s3_client.put_object(
            Bucket=bucket_name, Key=f"{folder_name}/{object_name}", Body=csv_buffer.encode())
    except ClientError as e:
        logging.error(e)
        return False
    return True