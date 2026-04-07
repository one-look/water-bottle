import logging
import sys
import time
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from src.etl.transformers import TransformerFactory
from src.etl.embedder import EmbedderFactory
from src.etl.loaders import LoaderFactory
from src.etl.credentials import CredentialFactory
from src.etl.connectors import ConnectorFactory
from src.etl.utils import load_yml

# Configure professional logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"etl_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("NMC_ETL_PIPELINE")

def main():
    start_time = time.time()
    logger.info("NMC ETL Pipeline Started")
    
    try:
        # 1. Load Configuration
        logger.info("Reading configuration...")
        config = load_yml('examples/nmc/etlnmc.yml')
        
        # 2. Load existing JSON data
        logger.info("Loading structured data from {}...".format(config.get("input", {}).get("file_path")))
        with open(config.get("input", {}).get("file_path"), 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        logger.info(f"Loaded {len(raw_data)} records from JSON file")
        
        # 3. Transformation
        logger.info("Starting data transformation and chunking...")
        transformer = TransformerFactory.get_transformer("file_json", raw_data, config.get("transformer", {}))
        transformed_data = transformer.transform(raw_data)
        
        embedder_data = transformed_data.get("embedder_data", [])
        logger.info(f"Successfully generated {len(embedder_data)} text chunks.")
        
        # 4. Embedding
        logger.info(f"Generating vectors using {config.get('embedder', {}).get('backend')}...")
        embedder = EmbedderFactory.get_embedder("gemini", embedder_data, config.get("embedder", {}))
        embedded_data = list(embedder.embed())
        
        # Debug: Check first record format
        if embedded_data:
            logger.debug(f"First embedded record keys: {list(embedded_data[0].keys())}")
            logger.debug(f"Vector field present: {'vector' in embedded_data[0]}")
            if 'vector' in embedded_data[0]:
                vector = embedded_data[0]['vector']
                logger.debug(f"Vector type: {type(vector)}, length: {len(vector) if vector else 'None'}")
                logger.debug(f"First 3 vector values: {vector[:3] if vector else 'None'}")
            else:
                logger.debug("No vector field found!")
        
        # 5. Loading to Pinecone
        logger.info("Initializing Pinecone connection...")
        pinecone_config = CredentialFactory.create("local", "pinecone").get_credentials()
        pinecone_connector = ConnectorFactory.create("pinecone", pinecone_config)
        pinecone_connection = pinecone_connector()
        
        loader = LoaderFactory.create("pinecone", pinecone_connection, config.get("loader", {}))
        
        logger.info(f"Upserting data to Pinecone index: {config.get('loader', {}).get('index_name')}")
        result = loader(embedded_data)
        
        # 6. Final Summary
        duration = round(time.time() - start_time, 2)
        upserted_count = result.get('upserted_count', len(embedded_data)) # Fallback if response varies
        
        logger.info("NMC ETL Pipeline Completed Successfully")
        logger.info(f"Total Duration: {duration} seconds")
        logger.info(f"Chunks Ingested: {upserted_count}")

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"PIPELINE FAILED: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()