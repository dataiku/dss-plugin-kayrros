import requests
import dataiku
import pandas as pd 
from utils.authentification import get_headers


def do(config, plugin_config):

    # Get credentials

    mode = plugin_config["preset"]["mode"]

    if mode == "INLINE":
        credentials = plugin_config["preset"]["inlinedConfig"]
        username = credentials["username"]
        password = credentials["password"]

    else:
        # If, for instance, mode == "PRESET"
        logger.exception("Preset mode is not implemented for now.")

        # Request the connections

    LIST_COLLECTIONS = 'https://platform.api.kayrros.com/v1/processing/collection/list'

    req = requests.get(LIST_COLLECTIONS, headers=get_headers(username, password))

    if req.status_code == 200:
        coll = req.json()

    choices = []

    # Build choices

    for item in coll:
        choices += [{"value":item["id"], "label":item["name"]}]

    return {"choices": choices}
