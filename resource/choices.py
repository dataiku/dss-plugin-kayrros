import requests
import dataiku
import pandas as pd 
from utils.authentification import get_headers


def do(config, plugin_config):

    # Request the connections
    
    LIST_COLLECTIONS = 'https://platform.api.kayrros.com/v1/processing/collection/list'

    print("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
    print(config,plugin_config)
    print("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")

    req = requests.get(LIST_COLLECTIONS, headers=get_headers(config, plugin_config))
    
    if req.status_code == 200:
        coll = req.json()
        
    choices = []

    # Build choices
    
    for item in coll:
        choices += [{"value":item["id"], "label":item["name"]}]
        
    return {"choices": choices}