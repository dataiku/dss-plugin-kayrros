import requests


def get_headers(config, plugin_config):
    
    #Retrieve token associated with email and password
    url = "https://auth.kayrros.com/v2/login"

    username = config["preset"]["username"]
    password = config["preset"]["password"]
    
    req = requests.post(url, json={"email": username, "password": password})
        
    if req.status_code == 200:
        token = req.json()['token']

    #Generate header to make further requests
    headers = {"Authorization": "Bearer " + token}
    return headers


