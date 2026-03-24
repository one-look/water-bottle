import logging
import time
import asyncio

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
    
    async def run(self, query: str, session_id: str = "internal_session") -> str:
        """Process a user query through the complete RAG pipeline.
        
        Args:
            query: User query string
            session_id: Session identifier for conversation history
            
        Returns:
            Generated response string
        """
        start_time = time.time()
        logger.info("action=workflow_start workflow=rag session_id=%s query_length=%d", session_id, len(query))
        
        try:
            # Check Redis cache first
            if hasattr(self.memory, 'get_cached_response'):
                cached_response = await self.memory.get_cached_response(query)
                if cached_response:
                    duration = time.time() - start_time
                    logger.info("action=workflow_cache_hit workflow=rag session_id=%s duration=%.3fs", session_id, duration)
                    return cached_response
            
            # Step 1: History (async)
            history_start = time.time()
            history = await self.memory.get_history(session_id)
            history_duration = time.time() - history_start
            logger.info("action=history_complete workflow=rag session_id=%s messages_count=%d duration=%.3fs", session_id, len(history), history_duration)
            
            # Step 2: Retrieval (async)
            retrieval_start = time.time()
            logger.info("action=retrieval_start workflow=rag session_id=%s", session_id)
            query_vector = await self.embedder.embed(query)
            search_results = await self.retriever.search(query_vector, top_k=5)
            context = [result.content for result in search_results]
            retrieval_duration = time.time() - retrieval_start
            logger.info("action=retrieval_complete workflow=rag session_id=%s documents_count=%d duration=%.3fs", session_id, len(context), retrieval_duration)
            
            # Step 3: Prompt & Generation (async)
            generation_start = time.time()
            logger.info("action=generation_start workflow=rag session_id=%s", session_id)
            prompt = self.prompt_manager.build_prompt(query, context, history)
            response = await self.generator.generate(prompt)
            generation_duration = time.time() - generation_start
            logger.info("action=generation_complete workflow=rag session_id=%s response_length=%d duration=%.3fs", session_id, len(response), generation_duration)
            
            # Step 4: Save to Redis cache
            if hasattr(self.memory, 'cache_response'):
                await self.memory.cache_response(query, response)
            
            # Step 5: Memory (async)
            memory_start = time.time()
            await asyncio.gather(
                self.memory.add_message(session_id, "user", query),
                self.memory.add_message(session_id, "assistant", response)
            )
            memory_duration = time.time() - memory_start
            logger.info("action=memory_complete workflow=rag session_id=%s duration=%.3fs", session_id, memory_duration)
            
            total_duration = time.time() - start_time
            logger.info("action=workflow_complete workflow=rag session_id=%s total_duration=%.3fs history=%.3fs retrieval=%.3fs generation=%.3fs memory=%.3fs", session_id, total_duration, history_duration, retrieval_duration, generation_duration, memory_duration)
            return response
            
        except Exception as e:
            total_duration = time.time() - start_time
            logger.error("action=workflow_failed workflow=rag session_id=%s duration=%.3fs error=%s", session_id, total_duration, str(e))
            return f"Error: {str(e)}"   