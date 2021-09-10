import requests
import dataiku
import pandas as pd 
from utils.authentification import get_headers


def do(payload, config, plugin_config, inputs):

    # Request the connections
    
    LIST_COLLECTIONS = 'https://platform.api.kayrros.com/v1/processing/collection/list'
    
    req = requests.get(LIST_COLLECTIONS, headers=get_headers(config))
   
    if req.status_code == 200:
        coll = req.json()
        
    choices = []
    
    # Build choices
    
    for item in coll:
        choices += [{"value":item["id"], "label":item["name"]}]
    
    return {"choices": choices}