import requests
import urllib3

def lambda_api_call(path):

    urllib3.disable_warnings()

    ## path: url of and API endpoint
    ## returns: dictionary

    requests.warnings = False

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
