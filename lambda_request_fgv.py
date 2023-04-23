import requests

def lambda_api_call(path):

    response = requests.get(path)


