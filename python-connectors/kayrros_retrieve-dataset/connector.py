# This file is the actual code for the custom Python dataset kayrros_retrieve-dataset

from dataiku.connector import Connector
from utils.authentification import get_headers
import pandas as pd
import requests
import logging

logger = logging.getLogger(__name__) 

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
        
        self.preset = self.config["preset"] # replace by self.id = config.get("id_dataset", "")
        if not self.preset:
            raise ValueError("A Kayrros account is necessary to fetch the data. Please provide one in the preset field.")
            
        print("000000000000000000000000000000000000000000000000000000000000")
        print(config)
        print(self.config["id_dataset"])
        print("000000000000000000000000000000000000000000000000000000000000")


        self.id = self.config["id_dataset"] #change that as suggested: self.id = config.get("id_dataset", "")
        if self.id == "":
            raise ValueError("A Kayrros dataset ID is necessary to fetch the data. Please provide one in the plugin settings.")
       
        username = self.preset["username"] # replace by self.id = config.get("id_dataset", "")
        password = self.preset["password"] # replace by self.id = config.get("id_dataset", "")

        self.headers = get_headers(username, password)
        
        
    def get_read_schema(self):

        return {"columns" : [ {"name": "value_date", "type" : "string"}, 
                              {"name" :"metrics", "type" : "array"},
                              {"name" :"extra_props", "type" : "array"},
                              {"name" :"name", "type" : "string"}]}


 #   def get_dataset(self):
        
  #      url_asset = "https://platform.api.kayrros.com/v1/processingresult/dataset/" + self.id

   #     try:        
     #       response = requests.post(url_asset, headers = self.headers)
    #        response.raise_for_status()
#        except requests.exceptions.RequestException as error:
 #           logger.exception("Dataset could not be retrieved because of the following error:\n {}".format(error))
  #          raise(error)
   #     content = response.json()
    #    logger.info("Received {} records".format(len(content["assets"])))

     #   return content


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """
        
        
        url_asset = "https://platform.api.kayrros.com/v1/processingresult/dataset/" + self.id

        try:        
            response = requests.post(url_asset, headers = self.headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            logger.exception("Dataset could not be retrieved because of the following error:\n {}".format(error))
            raise(error)
        content = response.json()
        logger.info("Received {} assets".format(len(content["assets"])))


   #     content = get_dataset(self)

        df = pd.DataFrame(content.get("assets",[]))
    
        for aggregated_item in ["results","metrics"]:
            df = df.explode(aggregated_item).reset_index().drop("index",axis=1)
            df = df.drop(aggregated_item,axis=1).join(df[aggregated_item].apply(pd.Series))
        
 #       list_aggreg = []

        # Shape the data
#        for asset in content.get("assets",[]):
 #           records = content["assets"][asset]["results"]
  #          for record in records:
   #             yield {"value_date" : str(record["value_date"]),
    #                   "metrics" : str(record["metrics"]),
     #                  "extra_props" : str(record["extra_props"]),
      #                 "name" : str(content["assets"][asset]["asset_name"])}

    
        for record in df.iterrows():
            yield record[1]

            
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

