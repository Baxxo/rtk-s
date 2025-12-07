from datetime import datetime
from kafka import KafkaProducer
import json
import time
import random
import threading
import sys
import os

# IP del PC dove gira Kafka
KAFKA_SERVER = '192.168.178.166:29092'

# Legge il numero di thread passati come argomento
if len(sys.argv) < 2:
    print("Uso: python producer.py <NUM_THREADS>")
    sys.exit(1)

NUM_THREADS = int(sys.argv[1])
print(f"Avvio di {NUM_THREADS} thread...")

# Evento per fermare i thread
stop_event = threading.Event()

# Contatori globali
messages_sent = 0
messages_failed = 0
counter_lock = threading.Lock()


def create_producer():
    """Crea un producer Kafka. Se Kafka non è disponibile, riprova finché non riesce."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Producer Kafka connesso.")
            return producer
        except Exception as e:
            print(f"Errore connessione Kafka: {e}. Riprovo in 1s...")
            time.sleep(1)


def send_temperature(thread_id):
    global messages_sent, messages_failed

    producer = create_producer()

    while not stop_event.is_set():
        temperatura = 20 + random.randint(0, 10)
        data = {
            "temperatura": temperatura,
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "thread_id": thread_id
        }

        try:
            producer.send("temperature", data)
            producer.flush()

            with counter_lock:
                messages_sent += 1

            print(f"[Thread {thread_id}] Inviato: {data}")

        except Exception as e:
            with counter_lock:
                messages_failed += 1

            print(f"[Thread {thread_id}] Errore invio: {e}. Riconnessione...")
            producer.close()
            producer = create_producer()

        time.sleep(1)

    producer.close()
    print(f"[Thread {thread_id}] Terminato.")


threads = []
for i in range(NUM_THREADS):
    t = threading.Thread(target=send_temperature, args=(i+1,), daemon=True)
    t.start()
    threads.append(t)

print(f"Avviati {NUM_THREADS} thread. PID: {os.getpid()}")

# Timer di 10 minuti
try:
    stop_event.wait(timeout=600)  # 600 secondi = 10 minuti
    stop_event.set()

    print("\nTempo scaduto: fermo tutti i thread...")
    for t in threads:
        t.join()

except KeyboardInterrupt:
    print("\nTerminazione richiesta dall'utente.")
    stop_event.set()
    for t in threads:
        t.join()

# STAMPA STATISTICHE FINALI
print("\n=== STATISTICHE ===")
print(f"Messaggi inviati: {messages_sent}")
print(f"Messaggi falliti: {messages_failed}")
print("===================")