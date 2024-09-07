from kafka import KafkaConsumer
import redis
import json
from prometheus_client import Counter, start_http_server


MESSAGE_CONSUMED_COUNT = Counter('kafka_messages_consumed_total', 'Total number of messages consumed from Kafka')
REDIS_WRITE_COUNT = Counter('redis_write_total', 'Total number of writes to Redis')

class ETLConsumer:
    def __init__(self, kafka_topics, redis_host='localhost', redis_port=6379, bootstrap_servers='localhost:9092'):
        self.kafka_topics = kafka_topics
        self.consumer = KafkaConsumer(
            *kafka_topics,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

    def transform_and_store(self):
        for message in self.consumer:
            data = message.value
            data['status'] = 'active' if data.get('completed') else 'inactive'
            key = f"task:{data['id']}"
            self.redis_client.set(key, json.dumps(data))
            MESSAGE_CONSUMED_COUNT.inc() 
            REDIS_WRITE_COUNT.inc()  
            print(f"Stored: {data}")

if __name__ == "__main__":
    kafka_topics = ["topic1", "topic2", 'topic3']

    
    start_http_server(8001)

    consumer = ETLConsumer(kafka_topics)
    consumer.transform_and_store()
