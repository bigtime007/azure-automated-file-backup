#https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/cosmos/azure-cosmos/samples/examples.py
from azure.cosmos import CosmosClient, PartitionKey
from azure.core.exceptions import AzureError
import logging
from config import config

log_name = 'app.log'
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)   
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', 
                            '%m-%d-%Y %H:%M:%S')

file_handler = logging.FileHandler(log_name)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class AzCosmosContainer:
    
    """Encapsulates an Azure Cosmos DB table of file name and time data."""
    
    def __init__(self, url, key, database_name, container_name):
        """
        :param container_name: Name of DynamDB Table.
        :param dyn_resource: A Boto3 DynamoDB resource. (Requred: import boto3)
        :self.table = None
        :self.response = None: for future pulling response after function run
        self.sort_key = 'fileName' : Set for current use, file name of the local scan
        self.sort_key_attr = 'fileTime' : Set for current use, file time of the local scan
        """
        self.url = url
        self.key = key
        self.container_name = container_name
        self.client = CosmosClient(self.url, self.key)
        self.database_name = database_name
        self.container = None
        self.partitionkey = "/partitionKey"
        self.response = None

    
    @property
    def create_load_db(self):
        
        """
        Creates an Amazon DynamoDB table that can be used to store file data.
        The table uses the sort_key as the sort key and currently has NO partition key.

        :param container_name: The name of the table to create.
        :return: The newly created table.

        """
        
        try:
            self.database = self.client.create_database_if_not_exists(
                id=self.database_name
                )            

        except AzureError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", self.container_name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise

    
    @property
    def create_load_container(self):
        
        """Allows for creation or loading a existing Container"""
        
        try:
            self.container = self.database.create_container_if_not_exists(
                id=self.container_name, 
                partition_key=PartitionKey(path=self.partitionkey)
                )
            
        except AzureError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", self.container_name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        

     
    def add_update_item(self, id_key_val, attr_val):
        """
        Adds a item to the table.

        :param sort_key_val: The Key of the item.
        :param sort_key_attr_val: The Attribute of the Key.
        Note: sort_key and sort_key_attr set by class __init__
        """
        
        try:
            id_key_val = id_key_val.replace("\\", "&")
            self.response = self.container.upsert_item(dict(id=id_key_val, fileTime=str(attr_val)))
            
            logger.info(f'Container: {self.container_name} Inserted: {self.response}')
            
        except AzureError as err:
            logger.error(
                "Couldn't add item %s to table %s. Here's why: %s: %s",
                id_key_val, attr_val, self.container_name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        
               
    def add_update_dictionary(self, dictionary):
        """
        Adds a items from a dict() to the table.

        :param dictionary: {'key' : value}
        """
        
        try:
            self.container = self.database.get_container_client(self.container_name)

            for key,value in dictionary.items():
                
                key = key.replace('/', '\\')
                
                self.response = self.container.upsert_item(dict(id=key.replace("\\", "&"), fileTime=str(value)))
                
                logger.info(f'Container: {self.container_name} Inserted: {self.response}')
                print(f'Container: {self.container_name} Inserted:', self.response)
                
        except AzureError as err:
            logger.error(
                "Couldn't add file %s to table %s. Here's why: %s: %s",
                dictionary, self.table.name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
            
    
    def delete_item(self, item, par_key={}):
        
        """
        Deletes a item from the table.

        :param item: The str() of the item to delete.
        :par_key: Default={} partition if needed
        """
        
        try:
            
            item = item.replace('/', '\\')
            
            self.response = self.container.delete_item(item=item.replace("\\", "&"), partition_key=par_key)
            
            logger.info(f'Item: {item} Deletion Complete')
            print(f'Item: {item} Deletion Complete')
        
        except AzureError as err:
            logger.error(
                "Couldn't delete item %s. Here's why: %s: %s", item, par_key,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise

    def delete_item_list(self, item_list, par_key={}):
        
        """
        Deletes a item from the table.

        :param item: The str() of the item to delete.
        :par_key: Default={} partition if needed
        """
        
        try:
            
            for item in item_list:
            
                item = item.replace('/', '\\')
                
                self.response = self.container.delete_item(item=item.replace("\\", "&"), partition_key=par_key)
                
                logger.info(f'Item: {item} Deletion Complete')
                print(f'Item: {item} Deletion Complete')
        
        except AzureError as err:
            logger.error(
                "Couldn't delete item %s. Here's why: %s: %s", item, par_key,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
    
    
    
    def delete_item_dict(self, dictionary, par_key={}):
        
        """
        Deletes a dict of items from the table.

        :param dictionary: dict of item to delete from table
        """
        
        try:
            for item in dictionary:
                
                item = item.replace("/", "\\")
                self.response = self.container.delete_item(item=item.replace("\\", "&"), partition_key=par_key)
                
                logger.info(f'Item: {item} Deletion Complete')
                print(f'Item: {item} Deletion Complete')
                
        except AzureError as err:
            logger.error(
                "Couldn't delete dictionary %s. Here's why: %s: %s", self.response,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
    
    
    def get_item(self, item, par_key={}):
        """
        Gets item data from the table for a specific file.

        :param sort_key_val: The value of the Key in Table..
        :return: The data about the requested file.
        """   
        try:
            self.response = self.container.read_item(item=item, partition_key=par_key)
            
            logger.info(self.response)
            
        except AzureError as err:
            logger.error(
                "Couldn't get file %s from table %s. Here's why: %s: %s",
                self.sort_key, self.table.name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        
        else:
            return self.response
    
    @property
    def scan_all_items(self):
        
        """
        Scans 'All Items' of the sort key.
        Uses a parses items in to dictionary format. 
        Requires load_table function called prior.
        
        :param not required.
        :return: The All Items in the specified container_name.
        """
        file_time_list = dict()
        
        try:    
            items = self.container.read_all_items()
            
            for item in items:
                
                file_time_list[item["id"].replace('&', '\\')] = float(item['fileTime'])
            
                logger.info(f'Item scan: {item}')
                        
        except AzureError as err:
            logger.error(
                "Couldn't scan for items. Here's why: %s: %s",
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        
        else:
            return file_time_list


if __name__ == "__main__":

    params = config()
    url = params["url"]
    key = params["key"]    

    container_name = "file-tracker"
    database_name = "file-tracker-db"
    
    dictionary = {'file11.txt': 464564564564, 'file22.txt': 54465454564, 'file33.txt': 454564564564564}
    item = dict(id="folder\\myfile2.txt", fileTime=str(464554564588))
    
    # Testing Class
    file_table = AzCosmosContainer(url=url, key=key, database_name=database_name, container_name=container_name)
    
    file_table.create_load_db
    file_table.create_load_container
    print(file_table.scan_all_items)
    #print(file_table.get_item('myfile3.txt'))
    #print(file_table.get_item(item='New Text Document.txt')["fileTime"])
    #print(file_table.add_update_dictionary({'foler example\\New Rich Text Document.rtf': 1670641367.9569840}))
    #print(file_table.scan_all_items)
    #print(file_table.add_update_item(id_key_val="myfile2.txt",attr_val=464554564588))
    #print(file_table.add_update_dictionary(dictionary=dictionary))
    #print(file_table.delete_item("folder\\myfile2.txt"))
    print(file_table.scan_all_items)
    #file_table.delete_item_dict(file_table.scan_all_items)
    
    
    ##file_table_2 = AzCosmosContainer(url=url, key=key, database_name=database_name, container_name="file-tracker-2")
    #file_table_2.create_load_db
    #file_table_2.create_load_container