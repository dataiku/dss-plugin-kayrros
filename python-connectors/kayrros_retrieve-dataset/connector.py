# This file is the actual code for the custom Python dataset kayrros_retrieve-dataset

from dataiku.connector import Connector
from utils.authentification import get_headers
import requests
import dataiku

class MyConnector(Connector):
    
    # A custom Python dataset is a subclass of connector.

    def __init__(self, config, plugin_config):
        
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        
        Connector.__init__(self, config, plugin_config) 

        # Perform some more initialization
        self.id = self.config["id_dataset"]
        self.preset = self.config["preset"]
        self.username = self.preset["username"]
        self.password = self.preset["password"]        
        
        
    def get_read_schema(self):

        return {"columns" : [ {"name": "value_date", "type" : "string"}, 
                              {"name" :"metrics", "type" : "array"},
                              {"name" :"extra_props", "type" : "array"},
                              {"name" :"name", "type" : "string"}]}


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """
        
        # I. Get the dataset from the id
        
        url_asset = "https://platform.api.kayrros.com/v1/processingresult/dataset/" + self.id

        req = requests.post(url_asset, headers = get_headers(self.config, self.plugin_config))

        if req.status_code == 200:
            content = req.json()
        
        else:
            raise Exception("Error retrieving dataset content")
        
        # II. Shape it and return its rows

        list_aggreg = []

        # Shape the data
        for i in range(len(content["assets"])):
            list_i = content["assets"][i]["results"]
            for item in list_i:
                item["name"] = content["assets"][i]["asset_name"]
            list_aggreg += list_i
        
        for record in list_aggreg:
            yield {"value_date" : str(record["value_date"]), 
                   "metrics" : str(record["metrics"]), 
                   "extra_props" : str(record["extra_props"]), 
                   "name" : str(record["name"])}

            
    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):
        """
        Returns a writer object to write in the dataset (or in a partition).

        The dataset_schema given here will match the the rows given to the writer below.

        Note: the writer is responsible for clearing the partition, if relevant.
        """
        raise Exception("Unimplemented")


    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        raise Exception("Unimplemented")


    def list_partitions(self, partitioning):
        """Return the list of partitions for the partitioning scheme
        passed as parameter"""
        return []


    def partition_exists(self, partitioning, partition_id):
        """Return whether the partition passed as parameter exists

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise Exception("unimplemented")


    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise Exception("unimplemented")

