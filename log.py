from config import config
import json
import logging
from logging.handlers import RotatingFileHandler
import requests
import urllib3

urllib3.disable_warnings() # using default cert.
params = config(section='splunk_log_config') # loads splunk log settings

class CustomHttpHandler(logging.Handler):
    
    def __init__(self, url:str, headers:dict, index:str) -> None:
        """Custom Handler for Splunk HEC *ONLY*

        Args:
            url (str): HTTP URL of Splunk Server
            headers (dict): Example: {"Authorization": "Splunk aaabbbcc-11111-22222-1x1x1-ab2ab2ab2"}
            index (str): Actual Splunk index name (in Splunk Portal under indexes)
        """
        self.url = url
        self.headers = json.loads(headers)
        self.index = index
        super().__init__()
    
    
    def emit(self, record:str) -> None:
        """This function gets called when a log event gets emitted. It receives a
        record, formats it and sends it to the url

        Args:
            record (str): a log record (created by logging module)
        """
        log_entry = self.format(record)
        response = requests.post(
            url=self.url, headers=self.headers, 
            json={"index": self.index, "event": log_entry}, 
            verify=False)
        print(response.status_code)

def setup_logger(logger_name:str=__name__, logfile:str='log.log') -> object:
    """Creates Logging Object: std out, log file w/1mb max, and Events sent to Splunk.

    Args:
        logger_name (str, optional): Logger Name.. Defaults to __name__. 
        logfile (str, optional): Log File Name. Defaults to 'log.log'.

    Returns:
        object: sets logging as a object.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    fh = RotatingFileHandler(filename=logfile, 
                             mode='a', 
                             maxBytes=1048576, 
                             backupCount=0, 
                             encoding=None, 
                             delay=False, 
                             errors=None
                             )
    fh.setLevel(logging.INFO)
    
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(levelname)s | %(message)s', 
        '%m-%d-%Y %H:%M:%S')    
    
    splunk_handler = CustomHttpHandler(**params)
    
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    splunk_handler.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.addHandler(splunk_handler)
    
    return logger

if __name__ == "__main__":
    logger=setup_logger('logger','log-sample.log')
    logger.info("My Logger has been initialized")

