from locust import HttpUser, task, between
import os
import uuid


class RAGUser(HttpUser):
    wait_time = between(1, 2)

    def on_start(self):
        client_id = os.environ.get("AUTH_CLIENT_ID", "water-bottle-client")
        client_secret = os.environ["AUTH_WATER_BOTTLE_CLIENT_SECRET"]
        email = os.environ.get("AUTH_TEST_USER_EMAIL", "p23dsc103@nmc.ac.in")
        response = self.client.post(
            "/auth/token",
            data={
                "grant_type": "user_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "email": email,
            },
        )
        response.raise_for_status()
        self.access_token = response.json()["access_token"]

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
