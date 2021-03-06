import requests
import logging
from utils.authentification import get_headers

logger = logging.getLogger(__name__) 


def do(payload, config, plugin_config, inputs):

    username = config.get("username", "")
    password = config.get("password", "")

    if payload.get('parameterName') == "collection_id":

        # Request the connections

        LIST_COLLECTIONS = "https://platform.api.kayrros.com/v1/processing/collection/list"

        response = requests.get(LIST_COLLECTIONS, headers=get_headers(username, password))

        # Build choices

        choices = []

        if response.status_code == 200:
            coll = response.json()
            for item in coll:
                choices += [{"value": item["id"], "label": item["name"]}]
        else:
            logger.exception("Collection could not be retrieved")

        return {"choices": choices}

    if payload.get("parameterName") == "dataset_id":

        GET_DATASETS = "https://platform.api.kayrros.com/v1/processing/collection/datasets"
        PARAMS = {"collection_id": config["collection_id"]}

        response = requests.post(GET_DATASETS, data=PARAMS, headers=get_headers(username, password))

        # Build choices

        choices = []

        if response.status_code == 200:
            ds = response.json()
            for item in ds:
                choices += [{"value": item["id"], "label": item["name"]}]

        else:
            logger.exception("Dataset could not be retrieved")

        return {"choices": choices}
