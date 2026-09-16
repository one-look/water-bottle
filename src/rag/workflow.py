"""Workflow service orchestrating high-performance RAG pipeline execution."""

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


def build_rag_prompt(
    query: str,
    documents: List[RetrivedDocument],
    history: List[Dict[str, str]],
) -> str:
    """Formats context documents, conversation history, and query into a structured prompt."""
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

    return f"""You are a helpful college assistant. Answer the user's question accurately using ONLY the provided context below. If the context does not contain enough information to answer factual questions, state clearly that you do not have enough information from the college records.

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

    async def execute(
        self, query: str, tenant_id: str, session_id: str
    ) -> Tuple[str, List[RetrivedDocument]]:
        """Executes query embedding, semantic cache lookup, retrieval, generation, and persistence asynchronously."""
        # Step 0: Fetch conversation history
        history = await self.memory_store.get_history(session_id)

        # Step 1: Embed query off event loop
        query_vector = await asyncio.to_thread(self.embedder.embed_query, query)

        # Step 2: Semantic cache lookup
        cached_answer = await self.cache_store.get(
            query_vector=query_vector,
            tenant_id=tenant_id,
            user_query=query,
        )

        if cached_answer:
            logger.info(f"Cache HIT: Returning cached response for tenant '{tenant_id}'.")
            await asyncio.gather(
                self.memory_store.add_message(session_id, role="user", content=query),
                self.memory_store.add_message(session_id, role="assistant", content=cached_answer),
            )
            return cached_answer, []

        # Step 3: Vector retrieval off event loop
        retrieved_docs = await asyncio.to_thread(
            self.retriever.retrieve, query_vector=query_vector, tenant_id=tenant_id
        )

        # Step 4: Build prompt & Generate LLM response
        prompt = build_rag_prompt(query=query, documents=retrieved_docs, history=history)
        generated_answer = await asyncio.to_thread(self.llm_provider.generate, prompt)

        # Step 5: Asynchronously save cache & update memory history concurrently
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