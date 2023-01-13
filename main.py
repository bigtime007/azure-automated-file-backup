import os, time, json
from config import config
from log import setup_logger
from azStorage import AZBlobStorage
from azCosmosContainer import AzCosmosContainer

logger=setup_logger(__name__)

class FileTracker:
    """Tracks assigned folder for add, delete, and file changes. 
    When detected executes upload to AZ Storage and places a record of files in CosmosDB of blob storage.
    """
    def __init__(self, 
                 working_dir: str, t_sec: str, conn_str: str, sto_container: str, 
                 db_name: str, uri: str, key: str, db_container: str
                 ):
        """Constructs all the necessary attributes: FileTracker object.

        Args:
            working_dir (str): Example: 'c:\\Users\\User123\\backup-folder'
            t_sec (str): time in seconds
            conn_str (str): Azure Blob Storage Connection String
            sto_container (str): Storage Actual Name
            db_name (str): Database Name in Azure CosmosDB
            uri (str): URI for CosmosDB connection
            key (str): Unique Key as Per Azure Acct.
            db_container (str): CosmosDB Actual Name
        """
        self.working_dir = working_dir
        self.t_sec = int(t_sec) 
        self.record_name = 'after-before-record.txt'
        
        # Blob Storage
        self.sto_container = sto_container
        self.conn_str = conn_str
        self.storage_resource = AZBlobStorage(
            working_dir=self.working_dir,conn_str=self.conn_str, 
            container=self.sto_container)
        
        # File Track DB
        self.db_name = db_name
        self.uri = uri
        self.key = key
        self.db_container = db_container
        self.db_resource = AzCosmosContainer(
            uri=self.uri, key=self.key, 
            database_name=self.db_name, container_name=self.db_container)
        self.db_load = self.db_resource.create_load_db
        self.db_container = self.db_resource.create_load_container
        
        
    @property   
    def file_list(self) -> list:
        """Creates a list of all files.
            Lists: Pwd and Sub-folder files, including hidden.           
 
        Returns:
            list: A list of path/file_name for working_dir
        """
        return [os.path.join(dirpath, file).replace(self.working_dir, "") for (
            dirpath, dirnames, filenames) in os.walk(self.working_dir) for file in filenames]
        
        
    @property
    def file_time_list(self) -> dict:
        """Creates a Dictionary of filenames as key and os time for values

        Returns:
            dict: {"path/filename": float}
        """
        file_list = self.file_list
        time_list = [os.path.getmtime(self.working_dir+file) for file in file_list]
        
        return dict(zip(file_list, time_list))
    
    
    def file_select_times(self, file_list: list, after_local: dict) ->dict:
        """Updates after_local dictionary items

        Args:
            file_list (list): List of files ["path/filename"]
            after_local (dict): Existing file time list

        Returns:
            dict: Updated after_local
        """
        time_list = [os.path.getmtime(self.working_dir+file) for file in file_list]
        update_after = dict(zip(file_list, time_list))
        after_local.update(update_after)

        return after_local
    
    
    @property
    def before_save_local(self) -> dict:
        """Loads file name self.record_name

        Returns:
            dict: {"filename": filetime}
        """
        try: 
            with open(self.record_name, 'r') as data:
                out = json.loads(data.read())

        except Exception as err:
            logger.error("Failed: %s Issue:" % err)

        return out
    
    
    def after_save_local(self, save:dict) ->None:
        """Saves self.record_name

        Args:
            save (dict): {"filename": filetime}
        """
        record = json.dumps(save)
        try:
            with open(self.record_name, 'w') as data:
                data.write(record)

        except Exception as err:
            logger.error("Failed: %s Issue" % err)
    
    
    def delete_files(self, file_list:dict) ->None: 
        """Deletes Local Files.

        Args:
            file_list (dict): {"filename": filetime}
        """
        for file in file_list:

            # Format path
            file_path = os.path.join(self.working_dir, file)
            # If file exists, delete it.
            if os.path.isfile(path=file_path):
                os.remove(path=file_path)
                logger.info("Success: %s file removed" % file_path)
                
            else:
                # If it fails, inform the user.
                logger.error("Error: %s file not found" % file_path)

            # Remove DIR   
            folder_path = file_path.replace(os.path.basename(file_path), '')
            if len(os.listdir(path=folder_path)) == 0:
                os.rmdir(path=folder_path, dir_fd = None)
                logger.info("Success: %s folder removed" % folder_path)
                
    
    @property
    def backup_svc(self) ->None:
        """Compares the state of the files from the last time the script was run, 
        and compares it with the current state of the files to detect changes, 
        ensuring that the files in the cloud storage 
        and the files in the local storage are always in sync.
        """
        before_local = self.before_save_local
        after_local = self.file_time_list
  
        logger.info("Scanning DB for Cloud Changes")
        before_cloud = self.db_resource.scan_all_items
        after_cloud = self.storage_resource.blob_file_time_list 
        
        print('-----------------------------------------------------------------------------------')
        logger.info(f"Local Directory file count before: {len(before_local)}")
      
        file_time_added_cloud = {key: value for key, value in after_cloud.items() if key not in before_cloud}
        file_time_removed_cloud = {key: value for key, value in before_cloud.items() if key not in after_cloud}
        file_time_changed_cloud = {key: value for key,value in after_cloud.items() if key in dict(
            set(before_cloud.items()) - set(after_cloud.items()))} 
        
        file_time_added_local = {key: value for key, value in after_local.items() if key not in before_local}   
        file_time_removed_local = {key: value for key, value in before_local.items() if key not in after_local}   
        file_time_changed = {key: value for key,value in after_local.items() if key in dict(
                    set(before_local.items()) - set(after_local.items()))} 
        file_time_changed_local = {key: value for key,value in file_time_changed.items() if key not in file_time_changed_cloud}
        
        ###################################################
        # before_cloud vs after_cloud if Added
        if file_time_added_cloud:
        # Action: download from remote storage. For: Added
            logger.info(f"|1| Cloud Added: Downloading: {file_time_added_cloud.keys()}")
            # Function to download files from Cloud
            self.storage_resource.get_list(file_time_added_cloud.keys())
            # updates local record
            self.file_select_times(file_list=file_time_added_cloud.keys(), after_local=after_local)
            # Function to update DB
            db_cloud_add = self.storage_resource.blob_file_select_time_list(file_time_added_cloud.keys())
            self.db_resource.add_update_dictionary(db_cloud_add)
        ####################################################
        # Check to see what was removed by another client in cloud.
        if file_time_removed_cloud: 
        # Action: remove file from: client
            logger.info(f"|2| Cloud Removed: Deleting locally: {file_time_removed_cloud.keys()}")
            # Function to Remove files from Folder in client
            self.delete_files(file_time_removed_cloud.keys())
            [after_local.pop(key) for key in file_time_removed_cloud.keys()]
            # Function to update DB
            self.db_resource.delete_item_list(file_time_removed_cloud.keys()) 
        ####################################################
        # What existing files have changed in Cloud since last scan
        if file_time_changed_cloud:
        # Action: Download from Storage 
            logger.info(f"|3| Cloud Changed: {file_time_changed_cloud.keys()}")
            # Function to add files from Storage
            self.storage_resource.get_list(file_time_changed_cloud.keys())
            # updates local record
            self.file_select_times(file_list=file_time_changed_cloud.keys(), after_local=after_local)
            # Function to update DB
            db_cloud_changed = self.storage_resource.blob_file_select_time_list(file_time_changed_cloud.keys())
            self.db_resource.add_update_dictionary(db_cloud_changed)
        ####################################################
        # files added to local 
        if file_time_added_local:
        # Action: upload to cloud and update db
            logger.info(f"|4| Local Added, Upload to Cloud: {file_time_added_local.keys()}")
            # Function to Upload files to Storage
            self.storage_resource.put_list(file_time_added_local.keys())
            # Function to update DB with current values
            db_cloud_add = self.storage_resource.blob_file_select_time_list(file_time_added_local.keys())
            self.db_resource.add_update_dictionary(db_cloud_add) 
        ####################################################
        # file removed from local
        if file_time_removed_local:
        # Action: Remove both object(s) from storage and Entry(s) from DB.
            logger.info(f"|5| Local Removed.. Delete Cloud: {file_time_removed_local.keys()}")
            # Function to Remove file for Remote Storage
            self.storage_resource.delete_list(file_time_removed_local.keys())
            # Function to Remove Entry from DB
            self.db_resource.delete_item_list(file_time_removed_local.keys())
        ####################################################
        # Existing file have changed in local.
        if  file_time_changed_local:
        # Action update files to storage if files were not changed in storage.
            logger.info(f"|6| Local Changed.. Update to Cloud: {file_time_changed_local.keys()}")
            # Func to update to Storage
            self.storage_resource.put_list(file_time_changed_local.keys())
            # func to update DB
            db_cloud_changed = self.storage_resource.blob_file_select_time_list(file_time_changed_local.keys())
            self.db_resource.add_update_dictionary(db_cloud_changed)
        ####################################################
        else:
            print('-----------------------------------------------------------------------------------')
            logger.info("No Local or Cloud File Changes Detected..")
            print('-----------------------------------------------------------------------------------')
               
        self.after_save_local(after_local) # Saves Changes to after_local
              
        logger.info(f'Local Directory file count after: {len(after_local)}')
        print('-----------------------------------------------------------------------------------')
        logger.info(f'All Done waiting:{self.t_sec} seconds.')
        time.sleep(self.t_sec)
        
        
params = config()
my_backup_folder = FileTracker(**params)

#while True:
for i in range(50):
    
    print('--------------------------------------------------------------------------------------------------------------------')
    print('Run:', i)
    my_backup_folder.backup_svc
    print('--------------------------------------------------------------------------------------------------------------------')