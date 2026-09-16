"""Workflow service for orchestrating RAG components."""

from typing import Any, Dict
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
    documents: list[RetrivedDocument],
    history: list[dict[str, str]],
) -> str:
    """Formats context documents, conversation history, and user query into a structured prompt."""
    if not documents:
        context_str = "No relevant context found in tenant database."
    else:
        context_blocks = [
            f"[Doc {idx + 1} - ID: {doc.document_id}]\n{doc.text}"
            for idx, doc in enumerate(documents)
        ]
        context_str = "\n\n".join(context_blocks)

    if not history:
        history_str = "No prior conversation history."
    else:
        history_blocks = [
            f"{msg['role'].capitalize()}: {msg['content']}" for msg in history
        ]
        history_str = "\n".join(history_blocks)

    return f"""You are a helpful multi-tenant enterprise assistant. Answer the user's question accurately using ONLY the provided context below. If the context does not contain enough information to answer, state clearly that you do not know based on the available data.

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
        self.config = config

    async def execute(
        self, query: str, tenant_id: str, session_id: str
    ) -> tuple[str, list[RetrivedDocument]]:
        """Executes query embedding, semantic cache lookup, retrieval, generation, and persistence."""
        # Step 0: Fetch memory history
        memory_store = MemoryFactory.create(self.config)
        history = await memory_store.get_history(session_id)

        # Step 1: Embed query
        embedder = EmbedderFactory.create(self.config)
        query_vector = embedder.embed_query(query)

        # Step 2: Check semantic cache
        cache_store = CacheFactory.create(self.config)
        cached_answer = await cache_store.get(
            query_vector=query_vector,
            tenant_id=tenant_id,
            user_query=query,
        )

        if cached_answer:
            logger.info(f"Cache HIT: Returning cached response for tenant '{tenant_id}'.")
            await memory_store.add_message(session_id, role="user", content=query)
            await memory_store.add_message(session_id, role="assistant", content=cached_answer)
            return cached_answer, []

        # Step 3: Vector retrieval
        retriever = RetrieverFactory.create(self.config)
        retrieved_docs = retriever.retrieve(query_vector=query_vector, tenant_id=tenant_id)

        # Step 4: Prompt generation
        prompt = build_rag_prompt(query=query, documents=retrieved_docs, history=history)

        # Step 5: LLM generation
        llm_provider = LLMFactory.create(self.config)
        generated_answer = llm_provider.generate(prompt)

        # Step 6: Persist results
        await cache_store.set(
            query_vector=query_vector,
            user_query=query,
            response_text=generated_answer,
            tenant_id=tenant_id,
        )
        await memory_store.add_message(session_id, role="user", content=query)
        await memory_store.add_message(session_id, role="assistant", content=generated_answer)

        return generated_answer, retrieved_docs