from src import lambda_libs as libs
import boto3
import json

def lambda_handler(event, context):

    """Upload a file to an S3 bucket

    :param event: Receive a dict of events
    :param context: Output of the execution
    :return: True if file was uploaded, else False
    """
    response = fgv_orgs(event)

def fgv_orgs(event):

    url = 'https://sistema-registropublicodeemissoes.fgv.br/'
    endpoint = '/public/organizations/'

    if event['orgs'] is None:

        fgv_orgs = libs.lambda_api_call(url + endpoint)

        for org_id in fgv_orgs:
            org_url = url + endpoint + str(org_id['_id'])
            org_detail = libs.lambda_api_call(org_url)

            # Upload the response
            try:
                upload_org = libs.upload_s3_object(
                    json_string=json.dumps(org_detail['organization']), 
                    bucket_name=event['bucket_name'], 
                    folder_name=event['org_folder'],
                    object_name=event['org_object'] + str(org_id['_id']).zfill(6) + '.json'
                )

                upload_inv = libs.upload_s3_object(
                    json_string=json.dumps(org_detail['inventories']), 
                    bucket_name=event['bucket_name'], 
                    folder_name=event['inv_folder'],
                    object_name=event['inv_object'] + str(org_id['_id']).zfill(6) + '.json'
                )
            except:
                return {
                    'status_code': 400,
                    'body' : 'Erro ao realizar upload do arquivo'
                }
        
        return {
            'status_code': 200,
            'body' : 'Upload realizado com sucesso'
        }
    else:

        for id in event['orgs']:
            org_url = url + endpoint + str(id)
            org_detail = libs.lambda_api_call(org_url)

            # Upload the response
            try:
                upload_org = libs.upload_s3_object(
                    json_string=json.dumps(org_detail['organization']), 
                    bucket_name=event['bucket_name'], 
                    folder_name=event['org_folder'], 
                    object_name=event['org_object'] + str(id).zfill(6) + '.json'
                )

                for inv in org_detail['inventories']:
                    upload_inv = libs.upload_s3_object(
                        json_string=json.dumps(inv), 
                        bucket_name=event['bucket_name'], 
                        folder_name=event['inv_folder'],
                        object_name=event['inv_object'] + str(id).zfill(6) + '-' + str([inv['_id']]).zfill(6) + '.json'
                    )
                
            except:
                return {
                    'status_code': 400,
                    'body' : 'Erro ao realizar upload do arquivo'
                }
        return {
            'status_code': 200,
            'body' : 'Upload realizado com sucesso'
        }

payload = {
    "bucket_name" : "uati-case-fgv",
    "org_folder" : "emissions-fgv-org",
    "org_object" : "emissions-fgv-org-",
    "inv_folder" : "emissions-fgv-inv",
    "inv_object" : "emissions-fgv-inv-",
    "orgs" : [1569, 990]
}
test = lambda_handler(event=payload, context=None)

##
## TODO
##
## |C| 1.0. Call the API endpoint "/organizations" to GET the JSON dict with all the companies IDs
## |C| 2.0. Call the API endpoint "/organizations/[_id]" to GET the JSON dict with all the companie details
## |C| 2.1. Get the JSON response and save in a S3 bucket
## |C| 2.1. Append the 