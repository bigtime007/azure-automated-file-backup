from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobClient
import logging


log_name = 'app.log'
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)   
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', 
                            '%m-%d-%Y %H:%M:%S')

file_handler = logging.FileHandler(log_name)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


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
        
    
    @staticmethod
    def create_container(new_container):
        """Creates AZ container @staticmethod
        
        :param new_container: new container string
        """
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str=conn_str)
            response = blob_service_client.create_container(new_container)

        except AzureError as err:
            logger.error(
                "Couldn't create AZ container %s. Here's why: %s: %s", new_container,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        
        finally:
            logging.info(response)
            logging.info(response.url)
            logging.info(response.account_name)
            logging.info(response.close) 
            return f'AZ Container:{new_container} Creation Completed'
        
    
    @staticmethod
    def delete_container(container):
        """Deletes AZ container @staticmethod
        
        :param container: container string
        """        
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str=conn_str)
            response = blob_service_client.delete_container(container=container)
        
        except AzureError as err:
            logger.error(
                "Couldn't delete AZ container %s. Here's why: %s: %s", container,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        
        finally:
            logging.info(response)
            logging.info(response.url)
            logging.info(response.account_name)
            logging.info(response.close) 
            logging.info(f'AZ Container Deletion Completed: {container}')
            return f'AZ Container:{container} Deletion Completed'

          
    def put_file(self, file_name):
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
            
            logging.info(f'Upload: {file_name}: {response}')
            
        except AzureError as err:
            logger.error(
                "Couldn't put AZ object %s. Here's why: %s: %s", file_name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
           
        return f"Uploaded: {file_name}: {response}"
            
     
    def put_list(self, file_list):
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
                logging.info(f'Upload: {response_list}')
        
        except AzureError as err:
            logger.error(
                "Couldn't put AZ object %s. Here's why: %s: %s", file_list,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        
        finally:     
            return response_list
    
    
    # Delete single file from AZ container
    def delete_object(self, blob):
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
                logging.info(response)
            
        except AzureError as err:
            logger.error(
                "Couldn't delete AZ object %s. Here's why: %s: %s", blob,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        
        finally:
            return response
    
    
    # Delete list of files from AZ container
    def delete_list(self, del_list):
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
                    response = f'Deletion Successful: {blob}'
                    logging.info(response)
                    response_list.append(response)
                          
        except AzureError as err:
            logger.error(
                "Couldn't delete AZ object %s. Here's why: %s: %s", del_list,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
        
        finally:
            return response_list
    
        
    
        
    # Get single Object from AZ
    def get_file(self, file_name):
        """Gets a single file/blob from container
        
        Args:
            :param key: str() of blob.
        
         Returns:
            FileObject: downloads to specific folder 
        """
        try:    
            with open(self.working_dir + file_name, "wb") as data:
                
                blob_client = BlobClient.from_connection_string(conn_str=self.conn_str, container_name=self.container, blob_name=file_name)
                blob_data = blob_client.download_blob()
                blob_data.readinto(data)
                
                response = f'Downloaded: {blob_data.properties}'
                logging.info(response)
            
        except AzureError as err:
            logger.error(
                "Couldn't get AZ object %s. Here's why: %s: %s", file_name,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise

        finally:
            return response
        
    
    
    def get_list(self, file_list):
        """Gets/Downloads a list of files/blobs from container
        
        Args:
            :param key: list() of filenames.
        
         Returns:
            FileObject: downloads to specific folder 
        """
        response_list = []
        
        try:

            for file_name in file_list:
                
                blob_client = BlobClient.from_connection_string(conn_str=self.conn_str, container_name=self.container, blob_name=file_name)
                    
                with open(self.working_dir + file_name, "wb") as data:

                    blob_data = blob_client.download_blob()
                    blob_data.readinto(data)
                    response = f'Downloaded: {blob_data.properties}'
                    logging.info(response)
                    response_list.append(response)
            
        except AzureError as err:
            logger.error(
                "Couldn't get AZ object %s. Here's why: %s: %s", file_list,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise

        finally:
            return response_list

    
    @property
    def blob_file_time_list(self):
        
        cloud_list = {}
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.conn_str)
            blob_client = blob_service_client.get_container_client(container=self.container)
            blob_names = blob_client.list_blobs()
            
            for blob in blob_names:
        
                cloud_list[blob.name] = blob.last_modified.timestamp()
        
        except AzureError as err:
            logger.error(
                "Couldn't get AZ object %s. Here's why: %s: %s", cloud_list,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
            
        finally:
            return cloud_list

        

    def blob_file_select_time_list(self, query_list):
        
        cloud_list = {}
        
        try:
            blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.conn_str)
            blob_client = blob_service_client.get_container_client(container=self.container)
            blob_names = blob_client.list_blobs()
            
            for blob in blob_names:
                if blob.name in query_list:
                    cloud_list[blob.name] = blob.last_modified.timestamp()
        
        except AzureError as err:
            logger.error(
                "Couldn't get AZ object %s. Here's why: %s: %s", cloud_list,
                err.message['Error']['Code'], err.message['Error']['Message'])
            raise
            
        finally:
            return cloud_list
    

if __name__ == "__main__":

    
    # Vars
    conn_str = "string"
    
    # Testing Class
    test = AZBlobStorage(conn_str=conn_str, container='file-sync-test', working_dir='c:\\Users\\Kevin\\bigtime007-github\\Azure file backup\\BACKUP-FOLDER\\')
    
    #AZBlobStorage.create_container(new_container="file-snyc-test-1312112")
    #AZBlobStorage.delete_container(container="file-snyc-test-1312112")
    print(test.blob_file_time_list)
    #print(test.put_file('/testfile.txt'))
    #print(test.put_list(file_list=['\\file2.txt', '\\testfile.txt']))
    #test.put_file('New Text Document.txt')
    #test.put_list(['New Text Document.txt'])
    #print(test.get_file("\\New Text Document.txt"))
    #print(test.get_list(['New Text Document.txt']))
    #print(test.get_list(['New folder\\New Text Document.txt']))
    #print(test.delete_object('/testfile.txt'))
    #print(test.delete_list(del_list=['foler example\\file3.txt']))
    #print(test.delete_list(del_list=['\\storagetool.py', '\\testfile.txt']))

    print(test.blob_file_select_time_list(['New Text Document.txt']))