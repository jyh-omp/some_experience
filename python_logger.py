import logging
import logging.handlers

def init_logger(logfile,loglevel):
    """
    init a logger for updater
    """
    #get a logger 
    logger = logging.getLogger()

    #create a handler to deal logger's info
    handler = logging.handlers.TimedRotatingFileHandler(logfile,"MIDNIGHT",1,15)
    handler.suffix = "%Y-%m-%d"
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(threadName)s] [%(module)s-%(funcName)s] %(message)s')
    handler.setFormatter(formatter)

    #init logger setting
    logger.addHandler(handler)
    logger.setLevel(loglevel)

    return logger
