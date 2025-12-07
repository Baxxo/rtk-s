from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import psycopg2
import time
import os

print('avvio consumer')

# --- Configurazione ---
KAFKA_SERVER = "localhost:29092"
TOPIC = "temperature"
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/temperature_db")

# --- Creazione del topic se non esiste ---
admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)

existing_topics = admin_client.list_topics()
if TOPIC not in existing_topics:
    topic = NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)
    admin_client.create_topics([topic])
    print(f"Topic '{TOPIC}' creato.")
else:
    print(f"Topic '{TOPIC}' già esistente.")

# --- Connessione a PostgreSQL ---
while True:
    try:
        conn = psycopg2.connect(DB_URL)
        break
    except psycopg2.OperationalError:
        print("DB non risponde...")
        time.sleep(2)

print("DB connesso")

cur = conn.cursor()

# --- Inizializzazione consumer Kafka ---
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# --- Loop principale ---
for message in consumer:
    data = message.value
    print(f"Ricevuto: {data}")
    cur.execute(
        "INSERT INTO temperature_log (temperatura, timestamp) VALUES (%s, %s)",
        (data['temperatura'], data['timestamp'])
    )
    conn.commit()
