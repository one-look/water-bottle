import logging

logger = logging.getLogger("water-bottle.workflow")

class RAGWorkflow:
    """Main RAG workflow orchestrator."""
    
    def __init__(self, embedder, retriever, memory, prompt_manager, generator):
        """Initialize RAG workflow with all required services.
        
        Args:
            embedder: Text embedding service
            retriever: Document retrieval service
            memory: Conversation memory service
            prompt_manager: Prompt management service
            generator: Text generation service
        """
        self.embedder = embedder
        self.retriever = retriever
        self.memory = memory
        self.prompt_manager = prompt_manager
        self.generator = generator
    
    def run(self, query: str, session_id: str = "internal_session") -> str:
        """Process a user query through the complete RAG pipeline.
        
        Args:
            query: User query string
            session_id: Session identifier for conversation history
            
        Returns:
            Generated response string
        """
        try:
            logger.info(f"Processing query for session: {session_id}")
            
            # Step 1: History
            history = self.memory.get_history(session_id)
            
            # Step 2: Retrieval
            logger.info("Generating embeddings and searching Elasticsearch...")
            query_vector = self.embedder.embed(query)
            search_results = self.retriever.search(query_vector, limit=5)
            context = [result.content for result in search_results]
            
            # Step 3: Prompt & Generation
            logger.info(f"Retrieved {len(context)} documents. Calling LLM...")
            prompt = self.prompt_manager.build_prompt(query, context, history)
            response = self.generator.generate(prompt)
            
            # Step 4: Memory
            self.memory.add_message(session_id, "user", query)
            self.memory.add_message(session_id, "assistant", response)
            
            return response
        except Exception as e:
            logger.error(f"Workflow Error: {str(e)}")
            return f"Error: {str(e)}"   