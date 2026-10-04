from locust import HttpUser, task, between
import os
import uuid


class RAGUser(HttpUser):
    wait_time = between(1, 2)

    def on_start(self):
        self.access_token = os.environ["GOOGLE_ID_TOKEN"]

    @task
    def test_rag_generate(self):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "X-Session-ID": str(uuid.uuid4()),
        }
        payload = {
            "query": "what are the available courses?",
        }

        self.client.post("/rag/generate", json=payload, headers=headers)
