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
    
    def process_query(self, query: str, session_id: str) -> str:
        """Process a user query through the complete RAG pipeline.
        
        Args:
            query: User query string
            session_id: Session identifier for conversation history
            
        Returns:
            Generated response string
        """
        try:
            # Step 1: Get conversation history
            history = self.memory.get_history(session_id)
            
            # Step 2: Embed the query
            query_vector = self.embedder.embed(query)
            
            # Step 3: Retrieve relevant documents
            search_results = self.retriever.search(query_vector, limit=5)
            context = [result.content for result in search_results]
            
            # Step 4: Build prompt with context and history
            prompt = self.prompt_manager.build_prompt(query, context, history)
            
            # Step 5: Generate response
            response = self.generator.generate(prompt)
            
            # Step 6: Store conversation in memory
            self.memory.add_message(session_id, "user", query)
            self.memory.add_message(session_id, "assistant", response)
            
            return response
            
        except Exception as e:
            error_msg = f"Error processing query: {str(e)}"
            # Store error in memory for debugging
            self.memory.add_message(session_id, "system", error_msg)
            return "I apologize, but I encountered an error while processing your request. Please try again."
