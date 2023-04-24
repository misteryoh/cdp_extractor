import requests
import json

def lambda_api_call(path):

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

def lambda_orgs():

    url = 'https://sistema-registropublicodeemissoes.fgv.br/public/organizations/'
    fgv_orgs = lambda_api_call(url)
