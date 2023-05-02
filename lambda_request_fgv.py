import lambda_libs as libs
import boto3
import json
import functools
import logging
import concurrent.futures

@libs.tempo_de_execucao
def lambda_handler(event, context):

    """ Main function to receive the payload and process the API calls
    
    :param event: Receive a dict payload
    :param context: Output of the execution
    :return: dict with status and message
    """

    url = 'https://sistema-registropublicodeemissoes.fgv.br/'
    endpoint = '/public/organizations/'

    api_endpoint = url + endpoint

    orgs_ids = event['orgs']
    proxies = event['proxies']

    # Check if payload contains pre-defined 'orgs'
    if orgs_ids is None:

        logging.info('Iniciando processamento sem orgs pre-definidas')

        fgv_orgs = libs.api_call(path=api_endpoint, proxies=proxies) # Receive API response

        logging.info('Obtendo lista de orgs para processamento')

        org_ids = [org_id['_id'] for org_id in fgv_orgs] # Iterate through dict to make orgs_id list

        logging.info('Iniciando processamento da lista de orgs')
        # Calls process_org to request Orgs details and upload to s3 bucket
        response = process_orgs(
            org_ids, 
            api_endpoint,
            event
        )

        logging.info('Finalizando processamento da lista de orgs')
        
        return response

    else: # If payload have pre-defined 'orgs'

        logging.info('Iniciando processamento com orgs pre-definidas')

        # Calls process_org to request Orgs details and upload to s3 bucket
        response = process_orgs(
            orgs_ids, 
            api_endpoint,
            event
        )

        logging.info('Finalizando processamento das orgs pre-definidas')
        
        return response

def process_orgs(org_ids, api_endpoint, event):
        
    """Process the API calls by Orgs IDs and upload the results to an S3 bucket

    :param orgs_ids: List of organizations IDs to iterate
    :param api_endpoint: API endpoint
    :param event: 
    :return: dict with status and message
    """

    call_upload_async = functools.partial(api_call_upload_s3, api_endpoint=api_endpoint, event=event)

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(call_upload_async, org_ids)
    
    # In case of sucess, return status_code 200
    return {
        'status_code': 200,
        'body' : 'Upload realizado com sucesso'
    }

def api_call_upload_s3(org_ids, api_endpoint, event):

    aws_profile = event['aws_profile']
    bucket_name = event['bucket_name']
    folder_name = event['folder_name']
    object_name = event['object_name']
    proxies     = event['proxies']

    org_url = api_endpoint + str(org_ids)
    org_detail = libs.api_call(path=org_url, proxies=proxies)

    # Try to upload the API response to S3 bucket/folder
    try:
        upload_org = libs.upload_s3_object(
            json_string=json.dumps(org_detail), 
            aws_profile=aws_profile,
            bucket_name=bucket_name, 
            folder_name=folder_name, 
            object_name=object_name + str(org_ids).zfill(6) + '.json'
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
    "aws_profile" : null,
    "bucket_name" : "uati-case-fgv",
    "folder_name" : "emissions-fgv-org",
    "object_name" : "emissions-fgv-org-",
    "proxies" : null,
    "orgs" : [1569, 990]
}

test = handler.lambda_handler(event=payload, context=None)
##
## TODO
##
## |C| 1.0. Call the API endpoint "/organizations" to GET the JSON dict with all the companies IDs
## |C| 2.0. Call the API endpoint "/organizations/[_id]" to GET the JSON dict with all the companie details
## |C| 2.1. Get the JSON response and save in a S3 bucket
