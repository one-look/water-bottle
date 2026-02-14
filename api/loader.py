from embedder.factory import EmbedderFactory
from retriever.search.factory import RetrieverFactory
from memory.factory import MemoryFactory
from prompt.behavior import PromptManager
from generators.factory import GeneratorFactory
from services.chatbot.workflow import RAGWorkflow
from elasticsearch import Elasticsearch

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
    
    def load_services(self) -> Dict[str, Any]:
        """Load and initialize all required services.
        
        Returns:
            Dictionary of initialized services
        """
        # Initialize Elasticsearch connection if configured
        if self.config.get("elasticsearch"):
            es_config = self.config["elasticsearch"]
            self._services["elasticsearch"] = Elasticsearch(
                hosts=es_config.get("hosts", ["localhost:9200"]),
                **es_config.get("options", {})
            )
        
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
