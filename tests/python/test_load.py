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
        # Simulates a user asking the Telegram bot a question
        payload = {
            "update_id": 123456,
            "message": {
                "message_id": 789,
                "from": {
                    "id": 987654321,
                    "is_bot": False,
                    "first_name": "Test User"
                },
                "chat": {
                    "id": 987654321,
                    "type": "private"
                },
                "date": 1672531200,
                "text": "Where is the admin office?"
            }
        }
        self.client.post("/api/v1/telegram/webhook", json=payload)