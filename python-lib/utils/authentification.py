import requests
import logging

logger = logging.getLogger(__name__)  

def get_headers(config, plugin_config):
    
    #Retrieve token associated with email and password
    url = "https://auth.kayrros.com/v2/login"

    username = config["preset"]["username"]
    password = config["preset"]["password"]
    
    try:        
        response = requests.post(url, json={"email": username, "password": password})
        response.raise_for_status()
    except requests.exceptions.RequestException as error:
        logger.exception("Authentication token could not be retrieved because of the following error:\n {}".format(error))
        raise(error)   
    token = response.json()['token']

    #Generate header to make further requests
    headers = {"Authorization": "Bearer " + token}
    return headers


