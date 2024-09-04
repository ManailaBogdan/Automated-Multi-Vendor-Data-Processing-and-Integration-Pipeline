import logging

def setup_logger():
    # Create a custom logger
    logger = logging.getLogger('global_logger')
    
    # Set the log level
    logger.setLevel(logging.INFO)
    logger.setLevel(logging.ERROR)
    logger.setLevel(logging.WARNING)
    logger.setLevel(logging.DEBUG)
    
    # Create handlers
    f_handler = logging.FileHandler('/var/log/app.log')
    f_handler.setLevel(logging.INFO)
    f_handler.setLevel(logging.ERROR)
    f_handler.setLevel(logging.WARNING)
    f_handler.setLevel(logging.DEBUG)
    
    
    # Create formatters and add them to handlers
    f_format = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s - %(lineno)d - %(process)d - %(thread)d - %(message)s')
    f_handler.setFormatter(f_format)
    
    # Add handlers to the logger
    logger.addHandler(f_handler)
    
    return logger

# Initialize the global logger
logger = setup_logger()



