from confluent_kafka import Consumer
import json
import snowflake.connector
import os
from dotenv import load_dotenv
from datetime import datetime

# Kafka Consumer Config
consumer_conf = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "postgres_to_snowflake_v1",
    # "auto.offset.reset": "earliest"
    "auto.offset.reset": "latest"

}

consumer = Consumer(consumer_conf)
consumer.subscribe(["postgres_cdc.public.products"])
load_dotenv()

print("Environment variables loaded.")
# print(os.getenv('user'))
# print()
# print()
# Snowflake Connection
conn = snowflake.connector.connect(
    user=os.getenv('user'),
    password=os.getenv('password'),
    account=os.getenv('account'),
    warehouse=os.getenv('warehouse'),
    database=os.getenv('database'),
    schema=os.getenv('schema')
)
cursor = conn.cursor()

def load_to_snowflake(row):
    sql = """
        INSERT INTO PRODUCTS (ID, NAME,DESCRIPTION,PRICE, UPDATED_DATE)
        VALUES (%s, %s, %s, %s, %s)
    """
    print(row)
    # exit()
    
    cursor.execute(sql, (
                row['id'],
                row['name'],
                row['description'],
                row['price'],
                datetime.now()
                ))
    conn.commit()

print("Listening for changes...")

while True:
    print("Polling for messages...")
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Error:", msg.error())
        continue

    payload = json.loads(msg.value())
    after = payload["payload"].get("after")
    print("Received change:", after)

    if after:
        load_to_snowflake(after)

cursor.close()
conn.close()
