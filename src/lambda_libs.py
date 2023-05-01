import requests
import urllib3
import logging
import time
import boto3
from botocore.exceptions import ClientError

def api_call(path, proxies):

    urllib3.disable_warnings()

    ## path: url of and API endpoint
    ## returns: dictionary

    requests.warnings = False

    # Open the Requests Session
    session = requests.Session()

    # GET request from the opened session
    try:
        if proxies is None:
            response = session.get(path, verify=False)
        else:
            response = session.get(path, verify=False, proxies=proxies)
    except:
        return False

    # Check if status_code == 200 to return the response
    if response.status_code == 200:
        return response.json()

def upload_s3_object(json_string, aws_profile, bucket_name, folder_name, object_name):
    """Upload an object to an S3 bucket

    :param json_string: JSON to upload
    :param folder: Bucket to upload to
    :param bucket_name: Folder to upload to
    :param object_name: S3 object name
    :return: True if object was uploaded, else False
    """

    # Create a session using the specified configuration file
    if aws_profile is None:
        session = boto3.Session()
    else:
        session = boto3.Session(profile_name=aws_profile)

    s3_client = session.client('s3')

    try:
        # Put object into the S3 bucker
        s3_object = s3_client.put_object(
            Bucket=bucket_name, Key=f"{folder_name}/{object_name}", Body=json_string)

        return s3_object
        
    except ClientError as e:
        logging.error(e)
        return False

def tempo_de_execucao(funcao):
    def wrapper(event, context):
        inicio = time.time()
        funcao(event, context)
        fim = time.time()
        tempo_execucao = (fim - inicio) / 60.0
        print("Tempo de execução:", tempo_execucao, "minutos")
    return wrapper
