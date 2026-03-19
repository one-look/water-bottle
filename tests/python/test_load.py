from locust import HttpUser, task, between

class User(HttpUser):
    # Simulates a user waiting 1 to 5 seconds between actions
    wait_time = between(1, 5)

    @task
    def check_health(self):
        # Hits your health endpoint
        self.client.get("/health")

    @task(3) # This task is 3x more likely to happen
    def ask_bot(self):
        # Simulates a user asking the bot a question - now using test endpoint
        payload = {
            "query": "Where is the admin office?",
            "session_id": f"test_session_{self.user_id}"
        }
        self.client.post("/api/v1/chat/test", json=payload)