import requests
import dataiku
import pandas as pd 
from utils.authentification import get_headers

def do(payload, config, plugin_config, inputs):
    

        # Get credentials

     #   mode = plugin_config["preset"]["mode"]

    #    if mode == "INLINE":
    #        credentials = plugin_config["preset"]["inlinedConfig"]
     #       username = credentials["username"]
      #      password = credentials["password"]

      #  else:
            # If, for instance, mode == "PRESET"
       #     logger.exception("Preset mode is not implemented for now.")

    username = config["username"]
    password = config["password"]
    
    
    
    
    
    if payload.get('parameterName') == 'collection_id':

            # Request the connections

        LIST_COLLECTIONS = 'https://platform.api.kayrros.com/v1/processing/collection/list'

        req = requests.get(LIST_COLLECTIONS, headers=get_headers(username, password))
        
        choices = []

        if req.status_code == 200:
            coll = req.json()
            for item in coll:
                choices += [{"value":item["id"], "label":item["name"]}]

        else:
            logger.exception("Collection could not be retrieved")

        # Build choices

        return {"choices": choices}

    
    
    
    
    
    if payload.get('parameterName') == 'dataset_id':
        
        GET_DATASETS = "https://platform.api.kayrros.com/v1/processing/collection/datasets"
        
        PARAMS = {"collection_id": config["collection_id"]}
        
        req = requests.post(GET_DATASETS, data=PARAMS, headers=get_headers(username,password))
        
        choices = []
        
        if req.status_code == 200:
            ds = req.json()
            for item in ds:
                choices += [{"value":item["id"], "label":item["name"]}]
           
        else:
            logger.exception("Dataset could not be retrieved")

        return {"choices": choices}

