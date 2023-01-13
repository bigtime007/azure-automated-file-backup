import configparser
from log import setup_logger
from azCosmosContainer import AzCosmosContainer
from azStorage import AZBlobStorage

logger = setup_logger()

class INICreator:
    """Creates config.ini for app configuration.
    """
    def __init__(self, file_name:str='config.ini') -> None:
        """_summary_Creates config.ini file

        Args:
            file_name (str, optional): _description_file name config.ini. Defaults to 'config.ini'.
        """
        self.file_name = file_name
        self.config = configparser.ConfigParser()
        self.record_name = 'after-before-record.txt'

    @property
    def create_local_db(self) ->None:
        """Creates local file for tracking file changes.
        """
        try:
            with open(self.record_name, 'x') as f:
                f.write()
                logger.info(f"Created: {self.record_name} File")
        except Exception as err:
            logger.error(f"Failed: {err}")
            
    @property    
    def create_config(self) -> object:
        """Allows for User to input strings for config

        Returns:
            object: Returns Modified ConfigParser object.
        """
        config_list = ["working_dir","t_sec","conn_str","sto_container","db_name","uri","key","db_container"]

        config = dict(zip(config_list, [input(f'{i}: ') for i in config_list ]))
        self.config["config"] = config

        print(config)
        logger.info('Created Config and User input accepted')

        return self.config

    @property    
    def create_splunk_log_config(self) -> object:
        """Allows for User to input strings for Splunk Log config

        Returns:
            object: Returns Modified ConfigParser object.
        """
        
        config_list = ["url","headers","index"]

        config = dict(zip(config_list, [input(f'{i}: ') for i in config_list ]))
        self.config["splunk_log_config"] = config

        print(config)
        logger.info('Created Config and User input accepted')

        return self.config

    @property
    def create_file(self) -> None:
        """Creates the config.ini file
        """
        try:
            with open(self.file_name, "w") as config_file:
                self.config.write(config_file)
                print("-----------------------------------")
                logger.info(f"{self.file_name}: Saved")

        except Exception as err:
            logger.error("Failed: %s Issue" % err)

class CreateAZResource:
    """Creates AZ Resource for Storage and DB Table.
    """
    def __init__(self, 
            working_dir: str, t_sec: str, conn_str: str, sto_container: str, 
            db_name: str, uri: str, key: str, db_container: str) -> None:
        
        self.working_dir = working_dir
        self.t_sec = int(t_sec) 
        
        self.sto_container = sto_container
        self.conn_str = conn_str
        self.storage_resource = AZBlobStorage(
            working_dir=self.working_dir,conn_str=self.conn_str, 
            container=self.sto_container)
        
        self.db_name = db_name
        self.uri = uri
        self.key = key
        self.db_container = db_container
        
        self.db_resource = AzCosmosContainer(
            uri=self.uri, key=self.key, 
            database_name=self.db_name, container_name=self.db_container)
   

    @property
    def create_db(self) -> None:
        """Cosmos DB Database Creator

        Returns:
            None: Creates DB
        """
        self.db_resource.create_load_db
        self.db_resource.create_load_container


    @property
    def create_sto_container(self) -> None:
        """Cosmos DB Container Creator for DB.

        Returns:
            None: Creates Container.
        """
        self.storage_resource.create_container(new_container=self.sto_container)
        

if __name__ == "__main__":

    #setup = INICreator('config-sam.ini')
    setup = INICreator()
    setup.create_config
    setup.create_splunk_log_config
    setup.create_file
    setup.create_local_db
    
    from config import config
    params = config(filename="config-sample.ini")

    az = CreateAZResource(**params)
    az.create_db
    az.db_container
    az.create_sto_container
    