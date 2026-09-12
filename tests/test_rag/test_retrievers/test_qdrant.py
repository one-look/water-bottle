import asyncio
from qdrant_client import QdrantClient
from qdrant_client import models as qmodels

from src.api.application import lifespan, get
from src.rag.embedders.factory import EmbedderFactory


async def run_diagnostics():
    """Runs direct vector retrieval diagnostics against Qdrant."""
    print("--- 1. INITIALIZING APPLICATION INSTANCE ---")
    
    # Trigger application context manager to set INSTANCE
    from fastapi import FastAPI
    dummy_app = FastAPI()
    
    async with lifespan(dummy_app):
        app_instance = get()
        if not app_instance:
            print("ERROR: Application instance failed to initialize.")
            return

        config = app_instance.config
        print(f"Loaded config: {config.get('qdrant', {})}")

        # Instantiate embedder via factory
        embedder = EmbedderFactory.create(config)
        
        query_text = "tell me about Ennum Ezhuthum Mission"
        print(f"\nGenerating query vector for: '{query_text}'...")
        query_vector = embedder.embed_query(query_text)
        print(f"Vector generated (dimensions: {len(query_vector)})")

        # Qdrant Client setup
        qdrant_cfg = config.get("qdrant", {})
        collection_name = qdrant_cfg.get("collection_name", "about_college")
        client = QdrantClient(url="http://localhost:6333")

        print("\n--- 2. TEST 1: UNFILTERED VECTOR SEARCH (NO TENANT FILTER) ---")
        try:
            unfiltered_res = client.query_points(
                collection_name=collection_name,
                query=query_vector,
                limit=5,
                score_threshold=0.0,  # Allow all scores
            )
            
            print(f"Found {len(unfiltered_res.points)} matches total in collection '{collection_name}':")
            for idx, p in enumerate(unfiltered_res.points):
                payload = p.payload or {}
                print(f"\n Match #{idx + 1}:")
                print(f"   - Point ID: {p.id}")
                print(f"   - Similarity Score: {p.score}")
                print(f"   - Stored Payload Keys: {list(payload.keys())}")
                print(f"   - tenant_id in payload: '{payload.get('tenant_id')}'")
                print(f"   - Text preview: {str(payload.get('text', ''))[:120]}...")

        except Exception as e:
            print(f"Unfiltered search failed: {e}")
            return

        print("\n--- 3. TEST 2: SEARCH WITH TENANT FILTER ('nmc') ---")
        try:
            filtered_res = client.query_points(
                collection_name=collection_name,
                query=query_vector,
                query_filter=qmodels.Filter(
                    must=[
                        qmodels.FieldCondition(
                            key="tenant_id",
                            match=qmodels.MatchValue(value="nmc"),
                        )
                    ]
                ),
                limit=5,
                score_threshold=0.0,
            )

            print(f"Found {len(filtered_res.points)} matches with tenant_id='nmc':")
            for idx, p in enumerate(filtered_res.points):
                print(f"   [{idx + 1}] Score: {p.score} | Tenant: {p.payload.get('tenant_id')}")

        except Exception as e:
            print(f"Filtered search failed: {e}")


if __name__ == "__main__":
    asyncio.run(run_diagnostics())