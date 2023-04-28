from uati_case_fgv.src import lambda_libs as libs
import boto3
import json

def lambda_handler(event, context):

    """ Main function to receive the payload and process the API calls
    
    :param event: Receive a dict payload
    :param context: Output of the execution
    :return: dict with status and message
    """

    url = 'https://sistema-registropublicodeemissoes.fgv.br/'
    endpoint = '/public/organizations/'

    # Check if payload contains pre-defined 'orgs'
    if event['orgs'] is None:

        fgv_orgs = libs.lambda_api_call(url + endpoint) # Receive API response

        org_ids = [org_id['_id'] for org_id in fgv_orgs] # Iterate through dict to make orgs_id list

        # Calls process_org to request Orgs details and upload to s3 bucket
        response = process_orgs(
            org_ids, 
            url, 
            endpoint, 
            event['bucket_name'], 
            event['folder_name'], 
            event['object_name']
        )
        
        return response

    else: # If payload have pre-defined 'orgs'

        # Calls process_org to request Orgs details and upload to s3 bucket
        response = process_orgs(
            event['orgs'], 
            url, 
            endpoint, 
            event['bucket_name'], 
            event['folder_name'], 
            event['object_name']
        )
        
        return response

def process_orgs(org_ids, url, endpoint, bucket_name, folder_name, object_name):
        
    """Process the API calls by Orgs IDs and upload the results to an S3 bucket

    :param orgs_ids: List of organizations IDs to iterate
    :param url: API path
    :param endpoint: API endpoint
    :param bucket_name: Name of the S3 bucket
    :param folder_name: Name of the S3 folder
    :param object_name: Name of the S3 object 
    :return: dict with status and message
    """

    for id in org_ids:

        org_url = url + endpoint + str(id)
        org_detail = libs.lambda_api_call(org_url)

        # Try to upload the API response to S3 bucket/folder
        try:
            upload_org = libs.upload_s3_object(
                json_string=json.dumps(org_detail), 
                bucket_name=bucket_name, 
                folder_name=folder_name, 
                object_name=object_name + str(id).zfill(6) + '.json'
            )   
        except:
            return {
                'status_code': 400,
                'body' : 'Erro ao realizar upload do arquivo'
            }
    # In case of sucess, return status_code 200
    return {
        'status_code': 200,
        'body' : 'Upload realizado com sucesso'
    }

payload = {
    "aws_profile" : None,
    "bucket_name" : "uati-case-fgv",
    "folder_name" : "emissions-fgv-org",
    "object_name" : "emissions-fgv-org-",
    "proxies" : None,
    "orgs" : [1569, 990]
}

# test = lambda_handler(event=payload, context=None)

##
## TODO
##
## |C| 1.0. Call the API endpoint "/organizations" to GET the JSON dict with all the companies IDs
## |C| 2.0. Call the API endpoint "/organizations/[_id]" to GET the JSON dict with all the companie details
## |C| 2.1. Get the JSON response and save in a S3 bucket
