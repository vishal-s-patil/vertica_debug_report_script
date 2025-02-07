import redis
import json

def connect_to_redis(host="localhost", port=6379, db=0):
    """Establish connection to Redis."""
    return redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)

def get_value(redis_client, key):
    """Retrieve value from Redis."""
    value = redis_client.get(key)
    if value:
        try:
            return json.loads(value) 
        except json.JSONDecodeError:
            return value 
    return None 

def put_value(redis_client, key, value):
    """Store JSON value in Redis."""
    if isinstance(value, (dict, list)): 
        value = json.dumps(value, default=str)
    redis_client.set(key, value)
