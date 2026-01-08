# Kayrros Plugin

This Dataiku plugin provides a read connector to interact with datasets stored on [Kayrros](https://platform.kayrros.com/).

**WARNING. This plugin is only available in the store from Dataiku 10.**

Kayrros is a data provider aiming at monitoring physical assets throughout the
world. It gives energy and industrial actors the right data to optimize their operations.

The Kayrros plugin for Dataiku lets you read from Kayrros’ [API](https://platform.kayrros.com/api-docs/), to include Kayrros’ expertise in your projects.

## How to use

Download the plugin. In the flow, click on **+ DATASET**, and **Kayrros**.

Enter:

- Your email and password used to connect to Kayrros’ API platform.
- The collection you want to retrieve (this will only be available if your Dataiku version is ≥ 9.0.5).
- A specific dataset in that collection (generally, they differ by the time granularity they offer).


Test the connection and don’t forget to save before returning to the “Explore” tab of the dataset.


## Troubleshooting

Have a look at Dataiku logs: backend.logs. You might want to filter on those logs using the terminal command:

```cat /path/to/DATA_DIR/run/backend.log | grep -i "kayrros"``` 

### **Frequent issues**

**My data isn’t accessible, I get a 504 error.**

It’s a known issue at Kayrros’ side – timeout errors happen when the file is too heavy for Kayrros’ API to return it. Please select another dataset to import or contact Kayrros for more information.
 

## Contact Kayrros

Please reach *contact@kayrros.com* for more information regarding what data Kayrros can offer.

### Licence

Copyright 2020-2021 Dataiku SAS

This plugin is distributed under the Apache License version 2.0
