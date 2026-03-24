import os
import logging
from embedder.factory import EmbedderFactory
from retriever.search.factory import RetrieverFactory
from memory.factory import MemoryFactory
from prompt.behavior import PromptManager
from generators.factory import GeneratorFactory
from services.rag.workflow import RAGWorkflow
from services.telegram.workflow import TelegramWorkflow

from typing import Dict, Any

logger = logging.getLogger(__name__)

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
        # Initialize Pinecone connection if configured
        if self.config.get("pinecone"):
            try:
                from pinecone import Pinecone
                import os
                
                # Get API key from environment
                api_key = os.getenv("PINECONE_API_KEY")
                if not api_key:
                    print("Warning: PINECONE_API_KEY environment variable not found")
                    # Create a dummy connection to prevent startup failure
                    self._services["pinecone"] = None
                else:
                    # Create Pinecone client
                    self._services["pinecone"] = Pinecone(api_key=api_key)
                    print("Pinecone connected successfully")
            except Exception as e:
                print(f"Warning: Failed to connect to Pinecone: {e}")
                # Create a dummy connection to prevent startup failure
                self._services["pinecone"] = None
        
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
            # Pass REDIS_URL to memory config for Redis cache
            memory_config = self.config["memory"].copy()
            if memory_config.get("redis_cache", False):
                redis_url = os.getenv("REDIS_URL")
                if redis_url:
                    memory_config["redis_url"] = redis_url
                    logger.info("action=memory_config memory_factory redis_url_provided=true")
                else:
                    logger.warning("action=memory_config memory_factory redis_url_missing redis_cache_disabled")
                    memory_config["redis_cache"] = False
            
            self._services["memory"] = MemoryFactory.create(
                memory_config, 
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
