import os
from embedder.factory import EmbedderFactory
from retriever.search.factory import RetrieverFactory
from memory.factory import MemoryFactory
from prompt.behavior import PromptManager
from generators.factory import GeneratorFactory
from services.rag.workflow import RAGWorkflow
from services.telegram.workflow import TelegramWorkflow
# from elasticsearch import Elasticsearch

from typing import Dict, Any

class AppLoader:
    """Application loader for initializing and managing global services."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize AppLoader with configuration.
        
        Args:
            config: Application configuration dictionary
        """
        self.config = config
        self._services = {}
        self._workflow = None
        self._telegram_workflow = None
    
    def load_services(self) -> Dict[str, Any]:
        """Load and initialize all required services.
        
        Returns:
            Dictionary of initialized services
        """
        # Initialize Elasticsearch connection if configured
        # Inside `AppLoader.load_services`
        # if self.config.get("elasticsearch"):
        #     try:
        #         es_config = self.config["elasticsearch"]
        #         
        #         # Get hosts from environment or config
        #         hosts = os.getenv("ELASTICSEARCH_URL") or es_config.get("hosts", ["https://localhost:9200"])
        #         if isinstance(hosts, str):
        #             hosts = [hosts]
        #         
        #         # Get auth from environment or config
        #         es_user = os.getenv("ELASTIC_USER") or es_config.get("options", {}).get("basic_auth", [None])[0]
        #         es_password = os.getenv("ELASTIC_PASSWORD") or es_config.get("options", {}).get("basic_auth", [None, None])[1]
        #         
        #         # Get verify_certs from environment or config
        #         verify_certs = os.getenv("ELASTIC_VERIFY_CERTS")
        #         if verify_certs is None:
        #             verify_certs = es_config.get("options", {}).get("verify_certs", False)
        #         else:
        #             verify_certs = verify_certs.lower() == "true"
        #         
        #         # Build connection options
        #         es_options = {}
        #         if es_user and es_password:
        #             es_options["basic_auth"] = [es_user, es_password]
        #         es_options["verify_certs"] = verify_certs
        #         
        #         self._services["elasticsearch"] = Elasticsearch(
        #             hosts=hosts,
        #             **es_options
        #         )
        #         # Verify connection immediately
        #         if self._services["elasticsearch"].ping():
        #             print("Elasticsearch connected successfully")
        #         else:
        #             print("Warning: Elasticsearch ping failed. Check credentials.")
        #     except Exception as e:
        #         print(f"Warning: Failed to connect to Elasticsearch: {e}")
        
        # Initialize Pinecone connection if configured
        if self.config.get("pinecone"):
            try:
                from src.etl.connectors import ConnectorFactory
                from src.etl.credentials import CredentialFactory
                
                # Create Pinecone connection
                pinecone_creds = CredentialFactory.create("local", "pinecone")
                pinecone_connector = ConnectorFactory.create("pinecone", pinecone_creds)
                self._services["pinecone"] = pinecone_connector()
                print("Pinecone connected successfully")
            except Exception as e:
                print(f"Warning: Failed to connect to Pinecone: {e}")
        
        # Initialize embedder
        if self.config.get("embedder"):
            self._services["embedder"] = EmbedderFactory.create(
                self.config["embedder"], 
                self._services
            )
        
        # Initialize retriever
        if self.config.get("retriever"):
            self._services["retriever"] = RetrieverFactory.create(
                self.config["retriever"], 
                self._services
            )
        
        # Initialize memory
        if self.config.get("memory"):
            self._services["memory"] = MemoryFactory.create(
                self.config["memory"], 
                self._services
            )
        
        # Initialize prompt manager
        if self.config.get("prompt"):
            self._services["prompt_manager"] = PromptManager(
                self.config["prompt"]
            )
        
        # Initialize generator
        if self.config.get("generator"):
            self._services["generator"] = GeneratorFactory.create(
                self.config["generator"], 
                self._services
            )
        
        return self._services
    
    def load_workflow(self) -> 'RAGWorkflow':
        """Load and initialize the RAG workflow.
        
        Returns:
            Initialized RAGWorkflow instance
        """
        if self._workflow is None:
            # Ensure services are loaded
            if not self._services:
                self.load_services()
            
            # Create workflow instance
            self._workflow = RAGWorkflow(
                embedder=self._services["embedder"],
                retriever=self._services["retriever"],
                memory=self._services["memory"],
                prompt_manager=self._services["prompt_manager"],
                generator=self._services["generator"]
            )
        
        return self._workflow
    
    def load_telegram_workflow(self) -> 'TelegramWorkflow':
        """Load and initialize the Telegram workflow.
        
        Returns:
            Initialized TelegramWorkflow instance
        """
        if self._telegram_workflow is None:
            # Ensure RAG workflow is loaded first
            if self._workflow is None:
                self.load_workflow()
            
            # Create Telegram workflow instance
            self._telegram_workflow = TelegramWorkflow(self._workflow, self.config)
        
        return self._telegram_workflow
    
    def get_services(self) -> Dict[str, Any]:
        """Get loaded services.
        
        Returns:
            Dictionary of services
        """
        return self._services.copy()
    
    def get_workflow(self) -> 'RAGWorkflow':
        """Get loaded workflow.
        
        Returns:
            RAGWorkflow instance
        """
        return self._workflow
    
    def get_telegram_workflow(self) -> 'TelegramWorkflow':
        """Get loaded Telegram workflow.
        
        Returns:
            TelegramWorkflow instance
        """
        return self._telegram_workflow
