from kafka import KafkaProducer
import json
import requests
import time
from prometheus_client import start_http_server, Counter, Histogram

class APIProducer:
    def __init__(self, api_url, kafka_topics, bootstrap_servers='localhost:9092'):
        self.api_url = api_url
        self.kafka_topics = kafka_topics
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.msg_counter = Counter('api_producer_messages_total', 'Total number of messages produced')
        self.error_counter = Counter('api_producer_errors_total', 'Total number of errors')
        self.api_request_duration = Histogram('api_producer_request_duration_seconds', 'Duration of API requests')

    def fetch_and_send(self):
        with self.api_request_duration.time():  
            try:
                response = requests.get(self.api_url)
                response.raise_for_status()
                data = response.json()
                for item in data:
                    for topic in self.kafka_topics:
                        self.producer.send(topic, value=item)
                        self.msg_counter.inc()  
                        print(f"Sent to {topic}: {item}")
                        time.sleep(1)  
            except Exception as e:
                self.error_counter.inc() 
                print(f"Error: {e}")

if __name__ == "__main__":
    #
    start_http_server(8000)

    api_url = "https://jsonplaceholder.typicode.com/todos"
    kafka_topics = ["topic1", "topic2", "topic3"]
    producer = APIProducer(api_url, kafka_topics)
    producer.fetch_and_send()
