import logging
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from src.etl.credentials import CredentialFactory
from src.etl.connectors import ConnectorFactory
from src.etl.extractors import ExtractorFactory
from src.etl.transformers import TransformerFactory
from src.etl.embedder import EmbedderFactory
from src.etl.loaders import LoaderFactory
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
        logger.info("Reading configuration from examples/nmc/config.yml...")
        config = load_yml('examples/nmc/config.yml')
        
        # 2. Setup Credentials
        logger.info("Fetching credentials from provider...")
        credential_provider = CredentialFactory.create("local", "nmc")
        creds = credential_provider.get_credentials()
        
        # 3. Connection & Extraction
        logger.info("Establishing connection to web source...")
        web_config = credential_provider.get_credentials()
        connector = ConnectorFactory.create("web", web_config)
        connection = connector()
        
        extractor = ExtractorFactory.get_extractor("web", connection, config.get("extractor", {}))
        logger.info(f"Extracting content from: {config.get('extractor', {}).get('url')}")
        raw_data = extractor.extract()
        
        # 4. Transformation
        logger.info("Starting data transformation and chunking...")
        transformer = TransformerFactory.get_transformer("web", raw_data, config.get("transformer", {}))
        transformed_data = transformer.transform(raw_data)
        
        embedder_data = transformed_data.get("embedder_data", [])
        logger.info(f"Successfully generated {len(embedder_data)} text chunks.")
        
        # 5. Embedding
        logger.info(f"Generating vectors using {config.get('embedder', {}).get('backend')}...")
        embedder = EmbedderFactory.get_embedder("gemini", embedder_data, config.get("embedder", {}))
        embedded_data = list(embedder.embed())
        
        # Debug: Check first record format
        if embedded_data:
            print(f"DEBUG: First embedded record: {embedded_data[0]}")
            print(f"DEBUG: First embedded record keys: {list(embedded_data[0].keys())}")
            if '_source' in embedded_data[0]:
                print(f"DEBUG: First _source keys: {list(embedded_data[0]['_source'].keys())}")
                print(f"DEBUG: Vector field present: {'vector' in embedded_data[0]['_source']}")
                if 'vector' in embedded_data[0]['_source']:
                    vector = embedded_data[0]['_source']['vector']
                    print(f"DEBUG: Vector type: {type(vector)}, length: {len(vector) if vector else 'None'}")
                    print(f"DEBUG: First 5 vector values: {vector[:5] if vector else 'None'}")
            else:
                print("DEBUG: No _source field found!")
        
        # 6. Loading to Pinecone
        logger.info("Initializing Pinecone connection...")
        pinecone_config = CredentialFactory.create("local", "pinecone").get_credentials()
        pinecone_connector = ConnectorFactory.create("pinecone", pinecone_config)
        pinecone_connection = pinecone_connector()
        
        loader = LoaderFactory.create("pinecone", pinecone_connection, config.get("loader", {}))
        
        logger.info(f"Upserting data to Pinecone index: {config.get('loader', {}).get('index_name')}")
        result = loader(embedded_data)
        
        # 7. Final Summary
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