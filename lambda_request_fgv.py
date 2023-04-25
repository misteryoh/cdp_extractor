from src import lambda_libs as libs
import boto3
import json

def lambda_handler(event, context):

    """Upload a file to an S3 bucket

    :param event: Receive a dict of events
    :param context: Output of the execution
    :return: True if file was uploaded, else False
    """

    bucket_name = 'uati-case-fgv'
    folder_name = 'emissions-orgs'
    object_name = 'emissions-orgs-'
    url = 'https://sistema-registropublicodeemissoes.fgv.br/public/organizations/'

    if event is None:

        fgv_orgs = libs.lambda_api_call(url)

        for org_id in fgv_orgs:
            org_url = url + str(org_id["_id"])
            org_detail = libs.lambda_api_call(org_url)

            # Upload the response
            upload_s3 = libs.upload_s3_object(
                json.dumps(org_detail["organization"]), 
                bucket_name, 
                folder_name, 
                object_name + str(org_id["_id"])
            )
            
    else:

        for id in event:
            org_url = url + str(id)
            org_detail = libs.lambda_api_call(org_url)

            # Upload the response
            upload_s3 = libs.upload_s3_object(
                json.dumps(org_detail["organization"]), 
                bucket_name, 
                folder_name, 
                object_name + str(id).zfill(6) + '.json'
            )

orgs = [1569, 990]
test = lambda_handler(event=orgs, context=None)

##
## TODO
##
## |C| 1.0. Call the API endpoint "/organizations" to GET the JSON dict with all the companies IDs
## |C| 2.0. Call the API endpoint "/organizations/[_id]" to GET the JSON dict with all the companie details
## |P| 2.1. Get the JSON response and save in a S3 bucket
## |P| 2.2. Get the the 