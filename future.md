1. Hybrid Search (Keyword + Vector)
Combines **BM25** (exact terms) with **Vector Search** (meaning). 
* **Goal:** Ensures that specific faculty names or IDs are never missed by fuzzy vector logic.

2. RAGAS & TruLens (Evaluation)
Frameworks to measure **Faithfulness** and **Relevance**.
* **Goal:** Quantifies if the bot is hallucinating or if Pinecone is retrieving the wrong chunks.

3. Re-Ranking
A second, smarter model (like **Cohere**) that re-orders the top 20 Pinecone results.
* **Goal:** Selects the absolute best 5 results to send to Gemini, improving accuracy.

4. Semantic Caching
Uses **Redis** or **GPTCache** to store query-answer pairs.
* **Goal:** If two users ask the same thing, the bot replies instantly without calling Gemini, saving costs.

5. Access Control (Whitelist)
A **SQLite** database checking `user_id` before processing.
* **Goal:** Blocks unauthorized users from consuming your API credits.

6. Rate Limiting
A "Cooldown" timer (e.g., 2 requests/sec) per `user_id`.
* **Goal:** Prevents a single user from spamming 1,000 requests and crashing your bot.

7. Query Expansion
An LLM "rewrites" vague user questions into detailed search queries.
* **Goal:** Turns "CS HOD" into "Who is the Head of the Department for Computer Science?"

8. Observability & Monitoring
Tools like **LangSmith** or **Arize Phoenix**.
* **Goal:** A live dashboard showing every user’s name, their query, and exact cost per request.
