from locust import HttpUser, task, between
import uuid

class RAGUser(HttpUser):
    wait_time = between(1, 2)

    @task
    def test_rag_generate(self):
        headers = {
            "X-Tenant-ID": "nmc",
            "X-Session-ID": str(uuid.uuid4())
        }
        payload = {
            "query": "what are the available courses?"
        }

        self.client.post("/rag/generate", json=payload, headers=headers)