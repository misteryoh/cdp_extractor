from src import api_requests
import boto3

def lambda_handler(event, context):

    """Upload a file to an S3 bucket

    :param event: Receive a dict of events
    :param context: Output of the execution
    :return: True if file was uploaded, else False
    """

    if event is None:

        url = 'https://sistema-registropublicodeemissoes.fgv.br/public/organizations/'

        fgv_orgs = api_requests.lambda_api_call(url)

        for org_id in fgv_orgs:
            org_url = url + str(org_id["_id"])
            org_detail = api_requests.lambda_api_call(org_url)

            # Upload the file
            # Create a session using the specified configuration file
            session = boto3.Session(profile_name='default')
            s3_client = session.client('s3')

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