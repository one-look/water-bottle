import os
from dotenv import load_dotenv
import logging

# Set up logger
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

CONNECTIONS = {
    "database": {
        "host": "localhost",
        "port": 5432,
        "database": "structured_data_pipeline",
        "login": "logi",
        "password": 12345,
    },
    "elasticsearch": {
        "host": "localhost",
        "port": 9200,
    },
    "pinecone": {
        "api_key": os.getenv("PINECONE_API_KEY") or ""
    }
}