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

        Connector.__init__(self, config, plugin_config) 

        # Retrieve credentials and token
        
        self.username = config.get("username","")
        if not self.username:
            raise ValueError("A Kayrros account is necessary to fetch the data. Please provide a username.")
        
        self.password = config.get("password","")
        if not self.password:
            raise ValueError("A Kayrros account is necessary to fetch the data. Please provide a password.")
        
        self.headers = get_headers(self.username, self.password)

        # Retrieve ids of collection and dataset
        
        self.collection_id = config.get("collection_id", "")
        if not self.collection_id :
            raise ValueError("Choosing a collection is necessary to fetch the data. Please provide one in the collection field.")

        self.dataset_id = config.get("dataset_id", "")
        if not self.password:
            raise ValueError("Choosing a dataset is necessary to fetch the data. Please provide one in the collection field.")


    def get_read_schema(self):
        
        # We don't specify a schema here, so DSS will infer the schema
        # from the columns actually returned by the generate_rows method

        return None


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        url_asset = "https://platform.api.kayrros.com/v1/processingresult/dataset/" + self.dataset_id
        
        try:        
            response = requests.post(url_asset, headers = self.headers)
            response.raise_for_status()
            
        except requests.exceptions.RequestException as error:
            logger.exception("Dataset could not be retrieved because of the following error:\n {}".format(error))
            
        content = response.json()
        
        # If no asset, abort it
        
        nb_assets = len(content["assets"])
        
        if(nb_assets == 0):
            logger.exception("No asset. Please contact Kayrros for access to data.")
        else:
            logger.info("Received {} assets".format(nb_assets))

        # Generate and format dataframe
        
        df = pd.DataFrame(content.get("assets",[]))
    
        for aggregated_item in ["results","metrics"]:
            df = df.explode(aggregated_item).reset_index().drop("index",axis=1)
            df = df.drop(aggregated_item,axis=1).join(df[aggregated_item].apply(pd.Series))
        
        
        # Old way :
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
            yield dict(record[1])

            
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

