import redis

class RedisSessionMemory:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

    def set(self, session_id, data, expiration=3600):  # expiration in seconds
        self.redis.setex(session_id, expiration, data)

    def get(self, session_id):
        return self.redis.get(session_id)

    def delete(self, session_id):
        self.redis.delete(session_id)

    def clear(self):
        self.redis.flushdb()  # Clear all sessions
