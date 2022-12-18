import os, time
import azStorage 
import azCosmosContainer
import json
from config import config
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


class FileTracker:
    """
    Tracks assigned folder for add, delete, and file changes. 
    When detected executes upload to S3 and places a record of file in DynamoDB table.
    Has State Lock Feature which allows for more than one client share the same S3 bucket.
    This feature is controlled by entry in state_lock table.
    """
    def __init__(self, 
                 working_dir: str, t_sec: str, conn_str: str, sto_container: str, db_name: str, url: str, key: str,  db_container: str
                 ):
        """Constructs all the necessary attributes: FileTracker object.

        Args:
            working_dir (str): Example: 'c:\\Users\\User123\\backup-folder'
            t_sec (int): time in seconds
        """
        self.working_dir = working_dir
        self.t_sec = int(t_sec) 
        self.record_name = 'after-before-record.txt'
        
        # Blob Storage
        self.sto_container = sto_container
        self.conn_str = conn_str
        self.storage_resource = azStorage.AZBlobStorage(
            working_dir=self.working_dir,conn_str=self.conn_str, container=self.sto_container)
        
        # File Track DB
        self.db_name = db_name
        self.url = url
        self.key = key
        self.db_container = db_container
        self.db_resource = azCosmosContainer.AzCosmosContainer(url=self.url, key=self.key, database_name=self.db_name, container_name=self.db_container)
        self.db_load = self.db_resource.create_load_db
        self.db_container = self.db_resource.create_load_container
        
        
    @property   
    def file_list(self):
        """ Creates a list of all files.
            Lists: Pwd and Sub-folder files, including hidden.           
            Requires: library's os, time        
        """
        return [os.path.join(dirpath, file).replace(self.working_dir, "") for (
            dirpath, dirnames, filenames) in os.walk(self.working_dir) for file in filenames]
        
        
    @property
    def file_time_list(self):
        """_summary_

        Returns:
            callable: _description_
        """
        file_list = self.file_list
        time_list = [os.path.getmtime(self.working_dir+file) for file in file_list]
        
        return dict(zip(file_list, time_list))
    
    
    def file_select_times(self, file_list: list, after_local: dict):
        
        time_list = [os.path.getmtime(self.working_dir+file) for file in file_list]
        
        update_after = dict(zip(file_list, time_list))

        after_local.update(update_after)

        return after_local
    
    
    @property
    def before_save_local(self):
        
        with open(self.record_name, 'r') as data:
            out = json.loads(data.read())
            
        return out
    
    
    def after_save_local(self, save):
        
        record = json.dumps(save)
        
        with open(self.record_name, 'w') as data:
            data.write(record)
    
    
    def delete_files(self, file_list):
        
        for file in file_list:
            
            if file in self.file_list:
            
                os.remove(self.working_dir + file)
            
            else:
                
                print("File already Removed")
        
        return "Files Deleted."
    
    @property
    def backup_svc(self):
        """ Summary:
        Detects file changes: add, remove, changed.
        Requires time setting: t_sec in seconds.
        Puts or deletes objects based on differences from before and after variable.
        'before' var. is created by scanning DB table, after by local file dir.
            Returns: 
                    
        """
        before_local = self.before_save_local
        after_local = self.file_time_list
  
        logger.info("Scanning DB for Changes")
        before_cloud = self.db_resource.scan_all_items
        after_cloud = self.storage_resource.blob_file_time_list 
        
        print('-----------------------------------------------------------------------------------')
        print(f"Local Directory file count before: {len(before_local)}")
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
        
        ####################################################
        # before_cloud vs after_cloud
        if file_time_added_cloud:
        # Action: download from remote storage. For: Added
            print("|1| Cloud Added... Downloading: ", ", ".join(file_time_added_cloud.keys()))
            logger.info(f"|1| Cloud Added: Downloading: {file_time_added_cloud.keys()}")
            # Function to download files from Cloud
            print(self.storage_resource.get_list(file_time_added_cloud.keys()))
            # updates local record
            self.file_select_times(file_list=file_time_added_cloud.keys(), after_local=after_local)
            # Function to update DB
            db_cloud_add = self.storage_resource.blob_file_select_time_list(file_time_added_cloud.keys())
            print(self.db_resource.add_update_dictionary(db_cloud_add))
            
        ####################################################
        # Check to see what was removed by another client in cloud.
        if file_time_removed_cloud: 
        # Action: remove file from: client
            print("|2| Cloud Removed... Deleting locally: ", ", ".join(file_time_removed_cloud.keys()))
            logger.info(f"|2| Cloud Removed: Deleting locally: {file_time_removed_cloud.keys()}")
            # Function to Remove files from Folder in client
            print(self.delete_files(file_time_removed_cloud.keys()))
            [after_local.pop(key) for key in file_time_removed_cloud.keys()]
            # Function to update DB
            print(self.db_resource.delete_item_list(file_time_removed_cloud.keys()))
            
        ####################################################
        # What has changed in Cloud since last scan
        if file_time_changed_cloud:
        # Action: upload to Storage 
            print("|3| Cloud Changed ", ", ".join(file_time_changed_cloud.keys()))
            logger.info(f"|3| Cloud Changed: {file_time_changed_cloud.keys()}")
            # Function to add files to Storage
            print(self.storage_resource.get_list(file_time_changed_cloud.keys()))
            # updates local record
            self.file_select_times(file_list=file_time_changed_cloud.keys(), after_local=after_local)
            # Function to update DB
            db_cloud_changed = self.storage_resource.blob_file_select_time_list(file_time_changed_cloud.keys())
            print(self.db_resource.add_update_dictionary(db_cloud_changed))
            
        ####################################################
        # file added to local 
        if file_time_added_local:
        # Action: upload to cloud and update db
            print("|4| Local Added, Upload to Cloud: ", ", ".join(file_time_added_local.keys()))
            logger.info(f"|4| Local Added, Upload to Cloud: {file_time_added_local.keys()}")
            # Function to Upload files to Storage
            print(self.storage_resource.put_list(file_time_added_local.keys()))
            # Function to update DB with current values
            db_cloud_add = self.storage_resource.blob_file_select_time_list(file_time_added_local.keys())
            print(self.db_resource.add_update_dictionary(db_cloud_add))
            
        ####################################################
        if file_time_removed_local:
        # Action: Remove both object(s) from storage and Entry(s) from DB.
            print("|5| Local Removed.. Delete Cloud: ", ", ".join(file_time_removed_local.keys()))
            logger.info(f"|5| Local Removed.. Delete Cloud: {file_time_removed_local.keys()}")
            # Function to Remove file for Remote Storage
            print(self.storage_resource.delete_list(file_time_removed_local.keys()))
            # Function to Remove Entry from DB
            print(self.db_resource.delete_item_list(file_time_removed_local.keys()))
            
        ####################################################
        if  file_time_changed_local:
        # Action update files to storage if files were not changed in storage.
            print("|6| Local Changed.. Update to Cloud: ", ", ".join(file_time_changed_local.keys()))
            logger.info(f"|6| Local Changed.. Update to Cloud: {file_time_changed_local.keys()}")
            # Func to update to Storage
            print(self.storage_resource.put_list(file_time_changed_local.keys()))
            # func to update DB
            db_cloud_changed = self.storage_resource.blob_file_select_time_list(file_time_changed_local.keys())
            print(self.db_resource.add_update_dictionary(db_cloud_changed))
            
        ####################################################
        else:
            print('-----------------------------------------------------------------------------------')
            print("No Local or Cloud File Changes Detected..")
            logger.info("No Local or Cloud File Changes Detected..")
            print('-----------------------------------------------------------------------------------')
               
        # Saves Changes to after_local
        self.after_save_local(after_local)
                
        print(f'Local Directory file count after: {len(after_local)}')   
        logger.info(f'Local Directory file count after: {len(after_local)}')
        print('-----------------------------------------------------------------------------------')
        
        print('All Done waiting:', f'{self.t_sec} seconds.')
        logger.info(f'All Done waiting:{self.t_sec} seconds.')
        time.sleep(self.t_sec)
        
        
params = config()
my_backup_folder = FileTracker(**params)

#while True:
for i in range(50):
    
    print('--------------------------------------------------------------------------------------------------------------------')
    print('Test:', i)
    my_backup_folder.backup_svc
    print('--------------------------------------------------------------------------------------------------------------------')