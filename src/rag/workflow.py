'''
Workflow service orchestrating high-performance RAG pipeline execution.
'''

import asyncio
from typing import Any, Dict, List, Tuple
from src.core.logging import setup_logger
from src.llm.factory import LLMFactory
from src.rag.cache.factory import CacheFactory
from src.rag.embedders.factory import EmbedderFactory
from src.rag.memory.factory import MemoryFactory
from src.rag.retrievers.factory import RetrieverFactory
from src.rag.retrievers.qdrant import RetrivedDocument

logger = setup_logger(__name__)


REWRITE_QUERY_PROMPT = """Given the conversation history and a user follow-up question, rewrite the follow-up question into a clear, complete, self-contained standalone search query that can be understood without the conversation history.
Do NOT answer the question. Only return the reformulated query in plain text. If the user question is already standalone, return it exactly as is.

Conversation History:
{history_str}

User Question: {query}
Standalone Search Query:"""


def build_rag_prompt(
    query: str,
    documents: List[RetrivedDocument],
    history: List[Dict[str, str]],
) -> str:
    '''
    Formats context documents, conversation history, and query into a structured prompt.

    
    '''
    
    if not history:
        history_str = "No prior conversation history."
    else:
        history_blocks = [
            f"{msg['role'].capitalize()}: {msg['content']}" for msg in history[-5:]
        ]
        history_str = "\n".join(history_blocks)

    if not documents:
        # Fallback prompt for conversational queries with zero retrieved context
        return f"""You are a helpful college assistant. 

Prior Conversation History:
{history_str}

User Question: {query}

Answer politely and helpfully:"""

    # RAG Context-bound prompt
    context_blocks = [
        f"[Doc {idx + 1} - ID: {doc.document_id}]\n{doc.text}"
        for idx, doc in enumerate(documents)
    ]
    context_str = "\n\n".join(context_blocks)

    return f"""You are a helpful college assistant. Answer the user's question clearly using ONLY the provided context below. 
    If the context is irrelevant to the college management system
    state clearly that you respond for the college management system queries.

Prior Conversation History:
---------------------
{history_str}
---------------------

Context Information:
---------------------
{context_str}
---------------------

User Question: {query}

Answer:"""


class RAGWorkflow:
    """Orchestrates end-to-end execution of the RAG pipeline."""

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize pipeline services once per workflow lifecycle."""
        self.config = config
        self.memory_store = MemoryFactory.create(config)
        self.embedder = EmbedderFactory.create(config)
        self.cache_store = CacheFactory.create(config)
        self.retriever = RetrieverFactory.create(config)
        self.llm_provider = LLMFactory.create(config)

    async def _rewrite_query_if_needed(self, query: str, history: List[Dict[str, str]]) -> str:
        """Reformulates contextual or ambiguous follow-up queries using conversation history."""
        if not history:
            return query

        history_blocks = [
            f"{msg['role'].capitalize()}: {msg['content']}" for msg in history[-4:]
        ]
        history_str = "\n".join(history_blocks)
        prompt = REWRITE_QUERY_PROMPT.format(history_str=history_str, query=query)

        try:
            rewritten = await asyncio.to_thread(self.llm_provider.generate, prompt)
            clean_rewritten = rewritten.strip().strip('"').strip("'")
            if clean_rewritten:
                logger.info(f"Query rewritten: '{query}' -> '{clean_rewritten}'")
                return clean_rewritten
        except Exception as e:
            logger.warning(f"Query rewriting failed: {e}. Falling back to original query.")

        return query

    async def execute(
        self, query: str, tenant_id: str, session_id: str
    ) -> Tuple[str, List[RetrivedDocument]]:
        """Executes query rewriting, pre-embed exact cache lookup, embedding, retrieval, generation, and persistence."""
        # Step 0: Fetch conversation history
        history = await self.memory_store.get_history(session_id)

        # Step 1: Pre-embed exact cache check (Zero latency/cost on exact query hits)
        cached_answer = None
        if hasattr(self.cache_store, "get_exact"):
            cached_answer = await self.cache_store.get_exact(tenant_id=tenant_id, user_query=query)

        if cached_answer:
            logger.info(f"Exact Cache HIT: Returning cached response for tenant '{tenant_id}'.")
            await asyncio.gather(
                self.memory_store.add_message(session_id, role="user", content=query),
                self.memory_store.add_message(session_id, role="assistant", content=cached_answer),
            )
            return cached_answer, []

        # Step 2: Contextual query rewriting when conversation history exists
        search_query = await self._rewrite_query_if_needed(query=query, history=history)

        # If rewritten query is different from raw query, check exact cache for rewritten query as well
        if search_query != query and hasattr(self.cache_store, "get_exact"):
            cached_answer = await self.cache_store.get_exact(tenant_id=tenant_id, user_query=search_query)
            if cached_answer:
                logger.info(f"Rewritten Query Cache HIT: Returning response for tenant '{tenant_id}'.")
                await asyncio.gather(
                    self.memory_store.add_message(session_id, role="user", content=query),
                    self.memory_store.add_message(session_id, role="assistant", content=cached_answer),
                )
                return cached_answer, []

        # Step 3: Embed search query off event loop (only executed on cache MISS)
        query_vector = await asyncio.to_thread(self.embedder.embed_query, search_query)

        # Step 4: Semantic cache lookup using query vector (for vector-based cache providers like RediSearch KNN)
        cached_answer = await self.cache_store.get(
            query_vector=query_vector,
            tenant_id=tenant_id,
            user_query=search_query,
        )

        if cached_answer:
            logger.info(f"Semantic Cache HIT: Returning response for tenant '{tenant_id}'.")
            await asyncio.gather(
                self.memory_store.add_message(session_id, role="user", content=query),
                self.memory_store.add_message(session_id, role="assistant", content=cached_answer),
            )
            return cached_answer, []

        # Step 5: Vector retrieval off event loop (dense or hybrid RRF) using search_query
        retrieved_docs = await self.retriever.retrieve(
            query_vector=query_vector,
            tenant_id=tenant_id,
            query_text=search_query,
        )

        # Step 6: Build prompt & Generate LLM response
        prompt = build_rag_prompt(query=query, documents=retrieved_docs, history=history)
        generated_answer = await asyncio.to_thread(self.llm_provider.generate, prompt)

        # Step 7: Asynchronously save cache & update memory history concurrently
        await asyncio.gather(
            self.cache_store.set(
                query_vector=query_vector,
                user_query=query,
                response_text=generated_answer,
                tenant_id=tenant_id,
            ),
            self.memory_store.add_message(session_id, role="user", content=query),
            self.memory_store.add_message(session_id, role="assistant", content=generated_answer),
        )

        return generated_answer, retrieved_docs