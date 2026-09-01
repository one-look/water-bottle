import logging
import sys
from src.config.settings import settings

def setup_logger(name: str) -> logging.Logger:
    '''
    Set up logger for a specific name.

    Args:
        name (str): Name of the logger
    
    Returns:
        logging.Logger: Logger instance
    '''
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(settings.LOG_LEVEL)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - [%(name)s] - [%(levelname)s] - [Tenant: %(tenant_id)s] - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger