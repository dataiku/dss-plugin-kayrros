import requests



def get_headers(config, plugin_config):

    #Retrieve token associated with email and password
    url = "https://auth.kayrros.com/v2/login"
    preset = config["preset"]
    
    
    print("000000000000000000000000000000000000000000000000000000000")
    print(config)

    print(plugin_config)

    print(preset)

    print("000000000000000000000000000000000000000000000000000000001")


    # POURQUOI DIABLE N'AI-JE PAS MES USERNAME & PASSWORD DANS LE PRESET ?
    username = "maxime.capron@dataiku.com"
    password = "Dataiku8Kayrros"

    req = requests.post(url, json={"email": username, "password": password})
        
    if req.status_code == 200:
        token = req.json()['token']

    #Generate header to make further requests
    headers = {"Authorization": "Bearer " + token}
    
    return headers


