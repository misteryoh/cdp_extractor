from src import lambda_libs as libs
import boto3
import json

def lambda_handler(event, context):

    """Upload a file to an S3 bucket

    :param event: Receive a dict of events
    :param context: Output of the execution
    :return: True if file was uploaded, else False
    """

    url = 'https://sistema-registropublicodeemissoes.fgv.br/public/organizations/'

    response = fgv_orgs(event)

def fgv_orgs(event):

    if event['orgs'] is None:

        fgv_orgs = libs.lambda_api_call(url)

        for org_id in fgv_orgs:
            org_url = url + str(org_id['_id'])
            org_detail = libs.lambda_api_call(org_url)

            # Upload the response
            try:
                upload_s3 = libs.upload_s3_object(
                    json_string=json.dumps(org_detail['organization']), 
                    bucket_name=event['bucket_name'], 
                    folder_name=event['folder_name'],
                    object_name=event['object_name'] + str(org_id['_id']) + '.json'
                )

                return {
                    'status_code': 200,
                    'body' : upload_s3
                }
            except ClientError as e:
                return {
                    'status_code': 400,
                    'body' : e
                }
    else:

        for id in event['orgs']:
            org_url = url + str(id)
            org_detail = libs.lambda_api_call(org_url)

            # Upload the response
            try:
                upload_s3 = libs.upload_s3_object(
                    json_string=json.dumps(org_detail['organization']), 
                    bucket_name=event['bucket_name'], 
                    folder_name=event['folder_name'], 
                    object_name=event['object_name'] + str(id).zfill(6) + '.json'
                )

                return {
                    'status_code': 200,
                    'body' : upload_s3
                }
            except ClientError as e:
                return {
                    'status_code': 400,
                    'body' : e
                }

def fgv_inventories(event):

    if event['orgs'] is None:

        fgv_orgs = libs.lambda_api_call(url)

        for org_id in fgv_orgs:
            org_url = url + str(org_id['_id'])
            org_detail = libs.lambda_api_call(org_url)

            # Upload the response
            try:
                upload_s3 = libs.upload_s3_object(
                    json_string=json.dumps(org_detail['organization']), 
                    bucket_name=event['bucket_name'], 
                    folder_name=event['folder_name'],
                    object_name=event['object_name'] + str(org_id['_id']) + '.json'
                )

                return {
                    'status_code': 200,
                    'body' : upload_s3
                }
            except ClientError as e:
                return {
                    'status_code': 400,
                    'body' : e
                }
    else:

        for id in event['orgs']:
            org_url = url + str(id)
            org_detail = libs.lambda_api_call(org_url)

            # Upload the response
            try:
                upload_s3 = libs.upload_s3_object(
                    json_string=json.dumps(org_detail['organization']), 
                    bucket_name=event['bucket_name'], 
                    folder_name=event['folder_name'], 
                    object_name=event['object_name'] + str(id).zfill(6) + '.json'
                )

                return {
                    'status_code': 200,
                    'body' : upload_s3
                }
            except ClientError as e:
                return {
                    'status_code': 400,
                    'body' : e
                }

payload = {
    "bucket_name" : "uati-case-fgv",
    "folder_name" : "emissions-orgs",
    "object_name" : "emissions-orgs-",
    "orgs" : [1569, 990]
}
test = lambda_handler(event=payload, context=None)

##
## TODO
##
## |C| 1.0. Call the API endpoint "/organizations" to GET the JSON dict with all the companies IDs
## |C| 2.0. Call the API endpoint "/organizations/[_id]" to GET the JSON dict with all the companie details
## |C| 2.1. Get the JSON response and save in a S3 bucket
## |C| 2.2. Get the the 