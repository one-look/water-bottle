import logging
from typing import List, Dict, Any
from pinecone import ServerlessSpec

class PineconeIngestor:
    def __init__(self, connection, config: Dict[str, Any]):
        """
        :param connection: The active Pinecone client object
        :param config: The 'loader' section of your YAML
        """
        self.client = connection  # This is the Pinecone() client instance
        self.config = config
        self.settings = config.get("settings", {})
        self.mappings = config.get("mappings", {})
        self.index_name = config.get("index_name", "default")
        self.logger = logging.getLogger(__name__)

        # --- AUTO-CREATE LOGIC ---
        self._ensure_index_exists()
        
        # Now get the actual index handle
        self.index = self.client.Index(self.index_name)

    def _ensure_index_exists(self):
        """Checks for index existence and creates it if missing."""
        existing_indexes = [idx.name for idx in self.client.list_indexes()]
        
        if self.index_name not in existing_indexes:
            self.logger.info(f"Index '{self.index_name}' not found. Creating new index...")
            
            # Extract specs from YAML or use defaults
            dim = self.settings.get("dimension", 384)
            metric = self.settings.get("metric", "cosine")
            spec_cfg = self.settings.get("spec", {})

            try:
                self.client.create_index(
                    name=self.index_name,
                    dimension=dim,
                    metric=metric,
                    spec=ServerlessSpec(
                        cloud=spec_cfg.get("cloud", "aws"),
                        region=spec_cfg.get("region", "us-east-1")
                    )
                )
                self.logger.info(f"Successfully created index: {self.index_name}")
            except Exception as e:
                self.logger.error(f"Failed to create index: {e}")
                raise e
        else:
            self.logger.info(f"Using existing Pinecone index: {self.index_name}")

    def _prepare_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transforms internal dict into Pinecone's required format."""
        id_key = self.mappings.get("id_key", "id")
        vector_key = self.mappings.get("vector_key", "vector")
        text_key = self.mappings.get("text_key", "text")
        meta_key = self.mappings.get("metadata_key", "metadata")

        # Handle both _source wrapped and flat formats
        if "_source" in record:
            # _source wrapped format
            source_data = record.get("_source", {})
            record_id = source_data.get(id_key)
            vector_values = source_data.get(vector_key)
            metadata = source_data.get(meta_key, {})
            raw_text = source_data.get(text_key, "")
        else:
            # Flat format (what we're actually getting)
            record_id = record.get(id_key)
            vector_values = record.get(vector_key)
            metadata = record.get(meta_key, {})
            raw_text = record.get(text_key, "")
        
        # Debug logging
        self.logger.debug(f"Record ID: {record_id}")
        self.logger.debug(f"Vector values type: {type(vector_values)}, is None: {vector_values is None}")
        
        # Inject the raw text into metadata so the bot can read it later
        metadata[text_key] = raw_text

        # Safety Check: Pinecone 40KB limit
        if len(str(metadata).encode('utf-8')) > 40000:
            self.logger.warning(f"Metadata for {record_id} exceeds 40KB. Truncating.")
            metadata[text_key] = raw_text[:10000] + "... [truncated]"

        return {
            "id": str(record_id),
            "values": vector_values,
            "metadata": metadata
        }

    def load(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not data:
            self.logger.info("No data provided to Pinecone loader.")
            return {"status": "empty"}

        upsert_payload = [self._prepare_record(r) for r in data]

        try:
            self.logger.info(f"Upserting {len(upsert_payload)} vectors to Pinecone...")
            response = self.index.upsert(vectors=upsert_payload, batch_size=100)
            return response
        except Exception as e:
            self.logger.error(f"Pinecone Upsert Failed: {str(e)}")
            raise e

    def __call__(self, data: List[Dict[str, Any]]):
        return self.load(data)