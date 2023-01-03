import logging
import requests

url = "https://127.0.0.1:8088/services/collector/event"
headers = {"Authorization": "Splunk 09584dbe-183b-4d14-9ee9-be66a37b331a"}
index = 'test_index'

class CustomHttpHandler(logging.Handler):
    
    def __init__(self, url:str, headers:dict, index:str) -> None:
    
        self.url = url
        self.headers = headers
        self.index = index
        super().__init__()
    
    
    def emit(self, record:str) -> exec:
        '''
        This function gets called when a log event gets emitted. It receives a
        record, formats it and sends it to the url
        Parameters:
            record: a log record (created by logging module)
        '''
        log_entry = self.format(record)
        response = requests.post(
            url=self.url, headers=self.headers, 
            json={"index": self.index, "event": log_entry}, 
            verify=False)


def setup_logger(logger_name:str=__name__, logfile:str='log.log'):
    """ Standard Logging: std out and log file.
        1.creates file handler which logs even debug messages: fh
        2.creates console handler with a higher log level: ch
        3.creates formatter and add it to the handlers: formatter, setFormatter
        4.adds the handlers to the logger: addHandler

    Args:
        logger_name (str, optional): Logger Name. Defaults to __name__.
        logfile (str, optional): Log File Name. Defaults to 'app.log'.

    Returns:
        logging: function to set logging as a object.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.INFO)
    
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(levelname)s | %(message)s', 
        '%m-%d-%Y %H:%M:%S')    
    
    splunk_handler = CustomHttpHandler(url=url, headers=headers, index=index)
    
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    splunk_handler.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.addHandler(splunk_handler)
    
    return logger

if __name__ == "__main__":
    logger=setup_logger('logger','app.log')
    logger.info("My Logger has been initialized")