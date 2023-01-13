#https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/cosmos/azure-cosmos/samples/examples.py

from config import config
from log import setup_logger
from azure.cosmos import CosmosClient, PartitionKey
from azure.core.exceptions import AzureError

logger=setup_logger(__name__)

class AzCosmosContainer:
    """Encapsulates an Azure Cosmos DB table: fileName and fileTime data.
    """
    def __init__(self, uri:str, key:str, database_name:str, container_name:str):
        """Required to implement the Azure DB container

        Args:
            uri (str): Database URI found in Cosmos DB/setting/keys
            key (str): Database KEY found in Cosmos DB/setting/keys
            database_name (str): Database Name
            container_name (str): Container name 
        """    
        self.uri = uri
        self.key = key
        self.container_name = container_name
        self.client = CosmosClient(self.uri, self.key)
        self.database_name = database_name
        self.container = None
        self.partitionkey = "/partitionKey"
        self.database = None
    
    @property
    def create_load_db(self):
        """Creates Database or loads if it exists.
        """
        try:
            self.database = self.client.create_database_if_not_exists(
                id=self.database_name
                )            
            logger.info(f'Created or Loaded DB: {self.database_name}')

        except AzureError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", self.container_name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
    

    @property
    def create_load_container(self):
        """Creates Container or loads if it exists.
        """
        try:
            self.container = self.database.create_container_if_not_exists(
                id=self.container_name, 
                partition_key=PartitionKey(path=self.partitionkey)
                )
            logger.info(f'Created or Loaded DB Container: {self.container_name}')

        except AzureError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", self.container_name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        
     
    def add_update_item(self, id_key_val:str, attr_val:float):
        """Adds or Updates a item in the cosmos DB Conatiner Table

        Args:
            id_key_val (str): fileName, Example: "myfile.txt"
            attr_val (float): fileTime, Example: 42425435345.4243
        """
        try:
            id_key_val = id_key_val.replace("\\", "&")
            response = self.container.upsert_item(dict(id=id_key_val, fileTime=str(attr_val)))
            
            logger.info(f'Container: {self.container_name} Inserted: {response}')
            
        except AzureError as err:
            logger.error(
                "Couldn't add item %s to table %s. Here's why: %s: %s",
                id_key_val, attr_val, self.container_name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        
               
    def add_update_dictionary(self, dictionary:dict):
        """Adds or Updates a dictionary of items in the cosmos DB Conatiner Table

        Args:
            dictionary (dict): Format: {fileName: fileTime}, 
            Example: {'file11.txt': 464564564.564, 'file2.txt': 54465454.564} 
        """
        try:
            self.container = self.database.get_container_client(self.container_name)

            for key,value in dictionary.items():
                
                key = key.replace('/', '\\')
                #key = os.path.abspath(key)
                
                response = self.container.upsert_item(dict(id=key.replace("\\", "&"), fileTime=str(value)))
                
                logger.info(f'Container: {self.container_name} Inserted: {response}')
                
        except AzureError as err:
            logger.error(
                "Couldn't add file %s to table %s. Here's why: %s: %s",
                dictionary, self.table.name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
            
    
    def delete_item(self, item:str, par_key={}):
        """Deletes a item from the table.

        Args:
            item (str): fileName, The str() of the item to delete. 
            par_key (dict, optional): Partition key. Defaults to {}.
        """
        try:
            
            item = item.replace('/', '\\')
            #item = os.path.abspath(item)
            
            response = self.container.delete_item(item=item.replace("\\", "&"), partition_key=par_key)
            
            logger.info(f'Item: {item} Deletion of Table Item Complete')
        
        except AzureError as err:
            logger.error(
                "Couldn't delete item %s. Here's why: %s: %s", item, par_key,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise

    
    def delete_item_list(self, item_list:list, par_key={}):
        """Deletes a item from the table.

        Args:
            item_list (list): fileName, The str() of the item to delete.
            par_key (dict, optional): Partition Key if needed. Defaults to {}.
        """
        try:
            
            for item in item_list:
            
                item = item.replace('/', '\\')
                #item = os.path.abspath(item)
                
                response = self.container.delete_item(item=item.replace("\\", "&"), partition_key=par_key)
                
                logger.info(f'Item: {item} Deletion of Table Item Complete {response}')
        
        except AzureError as err:
            logger.error(
                "Couldn't delete item %s. Here's why: %s: %s", item, par_key,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
    
    
    def delete_item_dict(self, dictionary:dict, par_key={}):
        """Deletes a dictionary of items from the table.

        Args:
            dictionary (dict): {fileName, fileTime}, dict of items to delete from table
            par_key (dict, optional): Partition Key if needed. Defaults to {}.
        """
        try:
            for item in dictionary:
                
                item = item.replace("/", "\\")
                #item = os.path.abspath(item)
                response = self.container.delete_item(item=item.replace("\\", "&"), partition_key=par_key)
                
                logger.info(f'Item: {item} Deletion Complete {response}')

                
        except AzureError as err:
            logger.error(
                "Couldn't delete dictionary %s. Here's why: %s: %s", response,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
    
    
    def get_item(self, item:str, par_key={}) -> dict:
        """Gets item data from the table for a specific file.

        Args:
            item (str): fileName, The value of the Key in Table.
            par_key (dict, optional): Partition Key if needed. Defaults to {}.

        Returns:
            dict: Format: {fileName:str, fileTime:float}
        """
        try:
            response = self.container.read_item(item=item, partition_key=par_key)
            
            logger.info(response)
            
        except AzureError as err:
            logger.error(
                "Couldn't get file %s from table %s. Here's why: %s: %s",
                self.sort_key, self.table.name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        
        else:
            return response


    @property
    def scan_all_items(self) -> dict:
        """Scans 'All Items and converts to dictionary format ' 

        Returns:
            dict: All items in the give container_name table. 
            Format: {fileName:str, fileTime:float}
        """
        file_time_list = dict()
        
        try:    
            items = self.container.read_all_items()
            
            for item in items:
                
                file_time_list[item["id"].replace('&', '\\')] = float(item['fileTime'])
                        
        except AzureError as err:
            logger.error(
                "Couldn't scan for items. Here's why: %s: %s",
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        
        else:
            return file_time_list


if __name__ == "__main__":

    params = config()
    uri = params["uri"]
    key = params["key"]    

    container_name = "file-tracker"
    database_name = "file-tracker-db"
    
    dictionary = {'file11.txt': 464564564564, 'file22.txt': 54465454564, 'file33.txt': 454564564564564}
    item = dict(id="folder\\myfile2.txt", fileTime=str(464554564588))
    
    # Testing Class
    file_table = AzCosmosContainer(uri=uri, key=key, database_name=database_name, container_name=container_name)
    
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
    
    
    ##file_table_2 = AzCosmosContainer(uri=uri, key=key, database_name=database_name, container_name="file-tracker-2")
    #file_table_2.create_load_db
    #file_table_2.create_load_container