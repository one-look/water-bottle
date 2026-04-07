from src.etl.credentials import CredentialFactory
from src.etl.connectors import ConnectorFactory
from src.etl.loaders import LoaderFactory

# Get credentials
pinecone_config = CredentialFactory.create("local", "pinecone").get_credentials()

# Create connector
connector = ConnectorFactory.create("pinecone", pinecone_config)

# Create loader with connection and config
config = {
    "index_name": "sample-index", 
    "settings": {"dimension": 3, "metric": "cosine"},
    "mappings": {}
}
loader = LoaderFactory.create("pinecone", connector(), config)

# Create the index first
loader.create()

# Ingest data
loader.load([{
    "id": "sample_id",
    "values": [1.0, 2.0, 3.0],
    "metadata": {"sample_metadata_key": "sample_metadata_value"}
}])

print("Data ingested successfully!")
