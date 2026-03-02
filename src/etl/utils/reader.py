"""
Provides utility functions for file handling, including loading configuration 
files (YAML) and serialized data (Pickle/JSON).
"""

import logging
import yaml
from pathlib import Path
from typing import Any, Dict, Union

logger = logging.getLogger(__name__)

def load_yml(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Reads and parses a YAML configuration file.

    Args:
        file_path (str): The system path to the .yml or .yaml file.

    Returns:
        Dict[str, Any]: The parsed configuration dictionary.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is not a YAML or parsing fails.
    """
    path = Path(file_path)

    if not path.exists():
        error_msg = f"Configuration file not found: {path.absolute()}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    if path.suffix not in ['.yml', '.yaml']:
        error_msg = f"File {path.name} is not a valid YAML file."
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:
        with path.open('r') as f:
            logger.info(f"Loading YAML configuration from: {path.name}")
            return yaml.safe_load(f)
    except yaml.YAMLError as e:
        error_msg = f"Error parsing YAML file {path.name}: {e}"
        logger.exception(error_msg)
        raise ValueError(error_msg)