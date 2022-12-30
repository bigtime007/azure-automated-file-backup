import logging

def setup_logger(logger_name:str=__name__, logfile:str='app.log'):
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
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

if __name__ == "__main__":
    logger=setup_logger('logger','app.log')
    logger.info("My Logger has been initialized")

