import logging
import sys
from src.config.settings import settings
from src.core.context import tenant_context

class TenantContextFilter(logging.Filter):
    '''

    '''
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "tenant_id"):
            # pull active tenant from context variable (defaults to "default")
            record.tenant_id = tenant_context.get()
        return True

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

        handler.addFilter(TenantContextFilter())
        
        formatter = logging.Formatter(
            '%(asctime)s - [%(name)s] - [%(levelname)s] - [Tenant: %(tenant_id)s] - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger