import pandas as pd
import json
import time
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Load dataset
df = pd.read_csv("drugs.csv")

# Stream row by row into Kafka topic
for i, row in df.iterrows():
    message = row.to_dict()  # Convert row to dictionary
    producer.send("drugs-topic", value=message)
    print(f"Sent record {i+1}: {message}")
    time.sleep(0.5)  # Delay for streaming effect (0.5 sec per message)

producer.flush()
producer.close()
print("Finished sending all records to Kafka!")

