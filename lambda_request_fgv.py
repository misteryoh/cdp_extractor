from src import api_requests
import boto3

def lambda_handler(event, context):

    # event: dict
    # context: dict

    if event is None:

        url = 'https://sistema-registropublicodeemissoes.fgv.br/public/organizations/'

        fgv_orgs = api_requests.lambda_api_call(url)

        for org_id in fgv_orgs:
            org_url = url + str(org_id["_id"])
            org_detail = api_requests.lambda_api_call(org_url)

            print(org_detail["organization"]["name"])
    else:

        for id in event:
            org_url = url + str(id)
            org_detail = api_requests.lambda_api_call(org_url)

            print(org_detail["organization"]["name"])        

orgs = [1569, 990]
test = lambda_handler(event=None, context=None)

##
## TODO
##
## |C| 1.0. Call the API endpoint "/organizations" to GET the JSON dict with all the companies IDs
## |C| 2.0. Call the API endpoint "/organizations/[_id]" to GET the JSON dict with all the companie details
## |P| 2.1. Get the JSON response and save in a S3 bucket
## |P| 2.2. Get the the 