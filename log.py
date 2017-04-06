import logging



def get_logger(name, logfile):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('/var/log/dpi_log/{0}.log'.format(logfile))
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s')
    ch.setFormatter(formatter)  
    fh.setFormatter(formatter)  
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger

