# https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob/samplessto

import os
from config import config
from log import setup_logger
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobClient

logger=setup_logger(__name__)

class AZBlobStorage:
    
    """Encapsulates an Azure Blob Storage Container."""
    
    def __init__(self, working_dir: str, conn_str: str, container: str):
        """
        :param container: container name. 'example-container'
        :param conn_str: str() found in Azure Console storage container key section.
        """
        self.working_dir = working_dir
        self.conn_str = conn_str
        self.container = container
        
    
    def create_container(self,new_container) ->str:
        """Creates AZ container 
        
        :param new_container: new container string
        """
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.conn_str)
            response = blob_service_client.create_container(new_container)

            logger.info(response.url)
            logger.info(response.account_name)
            logger.info(response.close) 
            return f'AZ Container:{new_container} Creation Completed'

        except AzureError as err:
            logger.error(
                "Couldn't create AZ container %s. Here's why: %s ", 
                new_container,
                err.message)
            #raise
          
    
    def delete_container(self, container) ->str:
        """Deletes AZ container 
        
        :param container: container string
        """        
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.conn_str)
            response = blob_service_client.delete_container(container=container)
        
        except AzureError as err:
            logger.error(
                "Couldn't delete AZ container %s. Here's why: %s ", 
                container,
                err.message)
        
        finally:
            logger.info(response)
            logger.info(response.url)
            logger.info(response.account_name) 
            logger.info(f'AZ Container Deletion Completed: {container}')
            return f'AZ Container:{container} Deletion Completed'

          
    def put_file(self, file_name) ->str:
        """Uploads or Puts a singe file to container.
        
        Args:
            :param key: str()  filename.
        
         Returns:
            str(): Call Back Status
        """
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.conn_str)
            blob_client = blob_service_client.get_blob_client(container=self.container, blob=file_name)
                
            with open(self.working_dir + "\\" + file_name, 'rb') as file_data:
                response = blob_client.upload_blob(file_data, overwrite=True)
            
            logger.info(f'Upload: {file_name}: {response}')
            
        except AzureError as err:
            logger.error(
                "Couldn't put AZ object %s. Here's why: %s. Here's why: %s ", 
                file_name,
                err.message)
            raise
           
        return f"Uploaded: {file_name}: {response}"
            
     
    def put_list(self, file_list) ->list:
        """Uploads or Puts a list of files to container
        
        Args:
            :param key: list of filenames.
        
         Returns:
            list(): Call Back Status
        """
        response_list = []
        
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.conn_str)
            
            for file in file_list:
                
                blob_client = blob_service_client.get_blob_client(container=self.container, blob=file)
                    
                with open(self.working_dir + "\\" + file, 'rb') as file_data:
                    response = blob_client.upload_blob(file_data, overwrite=True)

                response_list.append(f'Upload: {file}: {response}')
                logger.info(f'Upload: {response_list}')
        
        except AzureError as err:
            logger.error(
                "Couldn't put AZ object %s. Here's why: %s ", 
                file_list,
                err.message)
            raise
        
        finally:     
            return response_list
    
    
    # Delete single file from AZ container
    def delete_object(self, blob) -> str:
        """Deletes Single blob in AZ Container
        
        Args:
            :param key: string of object key(name)
        
         Returns:
            str(): Call Back Status
        """
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.conn_str)
            blob_client = blob_service_client.get_blob_client(container=self.container)
            response = blob_client.delete_blob(blob)

            if response is None:
                response = f'Deletion Successful: {blob}'
                logger.info(response)
            
        except AzureError as err:
            logger.error(
                "Couldn't delete AZ object %s. Here's why: %s ", 
                blob,
                err.message)
            raise
        
        finally:
            return response
    
    
    # Delete list of files from AZ container
    def delete_list(self, del_list) ->list:
        """Deletes a list of blobs from container
        
        Args:
            :param key: list of filenames.
        
         Returns:
            list(): Call Back Status
        """
        response_list = []
        try:
            for blob in del_list:
                
                blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.conn_str)
    
                blob_client = blob_service_client.get_container_client(container=self.container)
                response = blob_client.delete_blob(blob)

                if response is None:
                    response = f'Blob Deletion Successful: {blob}'
                    logger.info(response)
                    response_list.append(response)
                          
        except AzureError as err:
            logger.error(
                "Couldn't delete AZ object %s. %s. Here's why: %s ", 
                del_list,
                err.message)
            raise
        
        finally:
            return response_list
    
              
    # Get single Object from AZ
    def get_file(self, file_name) ->str:
        """Gets a single file/blob from container
        
        Args:
            :param key: str() of blob.
        
         Returns:
            FileObject: downloads to specific folder 
        """
        try:    
            
            path = file_name.replace(os.path.basename(file_name), '')
            
            if path:

                path = os.path.join(self.working_dir, path)

                if os.path.exists(path=path) == False:
                    os.mkdir(path,)
                    logger.info("Success: %s mkdir:" % path)

                else:
                    logger.info("iSpath: %s no mkdir done:" % path)
                    
            else:
                logger.info(f"no sub dir for: {file_name}")
                pass 
            
            with open(self.working_dir + file_name, "wb") as data:
                
                blob_client = BlobClient.from_connection_string(conn_str=self.conn_str, container_name=self.container, blob_name=file_name)
                blob_data = blob_client.download_blob()
                blob_data.readinto(data)
                response = f'Downloaded: {blob_data.properties}'
                logger.info(response)
            
        except AzureError as err:
            logger.error(
                "Couldn't get AZ object %s. Here's why: %s: %s", file_name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise

        finally:
            return response
        
    
    def get_list(self, file_list) ->list:
        """Gets/Downloads a list of files/blobs from container
        
        Args:
            :param key: list() of filenames.
        
         Returns:
            FileObject: downloads to specific folder 
        """
        response_list = []
        try:
            
            for file_name in file_list:
                
                path = file_name.replace(os.path.basename(file_name), '')
                
                if path:
                    path = os.path.join(self.working_dir, path)

                    if os.path.exists(path=path) == False:
                        os.mkdir(path,)
                        logger.info("Success: %s mkdir:" % path)
                    else:
                        logger.info("iSpath: %s no mkdir done:" % path)
                        
                else:
                    logger.info("no sub dir: %s mkdir:" % path)
                    pass 
                
                blob_client = BlobClient.from_connection_string(conn_str=self.conn_str, container_name=self.container, blob_name=file_name)
                  
                with open(self.working_dir + file_name.replace('/', '\\'), "wb") as data:
                                    
                    blob_data = blob_client.download_blob()
                    blob_data.readinto(data)
                    response = f'Downloaded: {blob_data.properties}'
                    logger.info(response)
                    response_list.append(response)
            
        except AzureError as err:
            logger.error(
                "Couldn't get AZ object %s. Here's why: %s: %s", file_list,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise

        finally:
            return response_list

    
    @property
    def blob_file_time_list(self) ->list:
        
        cloud_list = {}
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.conn_str)
            blob_client = blob_service_client.get_container_client(container=self.container)
            blob_names = blob_client.list_blobs()
            
            for blob in blob_names:
                
                blob_name = blob.name
                blob_name = blob_name.replace("/", "\\")
                cloud_list[blob_name] = blob.last_modified.timestamp()

        except AzureError as err:
            logger.error(
                "Couldn't get AZ object %s. Here's why: %s: %s", cloud_list,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
            
        finally:
            return cloud_list
        

    def blob_file_select_time_list(self, query_list) ->list:
        
        cloud_list = {}    
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.conn_str)
            blob_client = blob_service_client.get_container_client(container=self.container)
            blob_names = blob_client.list_blobs()

            for blob in blob_names:
                blob_name = blob.name
                blob_name = blob_name.replace("/", "\\")
                if blob_name in query_list:
                    cloud_list[blob_name] = blob.last_modified.timestamp()
        
        except AzureError as err:
            logger.error(
                "Couldn't get AZ object %s. Here's why: %s: %s", cloud_list,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
            
        finally:
            return cloud_list
    

if __name__ == "__main__":

    
    # Vars
    params = config()
    conn_str = params["conn_str"]
    
    # Testing Class
    test = AZBlobStorage(conn_str=conn_str, container='file-sync-test', working_dir='c:\\Users\\Kevin\\bigtime007-github\\Azure file backup DEV ONLY\\BACKUP-FOLDER\\')
    
    test.create_container(new_container="file-snyc-test-1312112")
    #AZBlobStorage.delete_container(container="file-snyc-test-1312112")
    print(test.blob_file_time_list)
    #print(test.put_file('/testfile.txt'))
    #print(test.put_list(file_list=['\\file2.txt', '\\testfile.txt']))
    #test.put_file('New Text Document.txt')
    #test.put_list(['New folder\\New Text Document.txt'])
    #print(test.get_file("\\New Text Document.txt"))
    #print(test.get_list(['New Text Document.txt']))
    #print(test.get_list(['New folder\\New Text Document.txt','New folder\\New Text Document (2).txt']))
    #print(test.delete_object('/testfile.txt'))
    ##print(test.delete_list(del_list=['New folder\\New Text Document.txt']))
    #print(test.delete_list(del_list=['\\storagetool.py', '\\testfile.txt']))

    #print(test.blob_file_select_time_list(['Test Folder\\bookmarks.txt']))
    #print(test.blob_file_time_list)