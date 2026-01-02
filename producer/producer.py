from datetime import datetime
from kafka import KafkaProducer  # type: ignore
import json
import time
import random
import threading
import sys
import os

# IP del PC dove gira Kafka
KAFKA_SERVER = '192.168.178.166:29092'

if len(sys.argv) < 2:
    print("Uso: python producer.py <NUM_THREADS>")
    sys.exit(1)

NUM_THREADS = int(sys.argv[1])
print(f"Avvio di {NUM_THREADS} thread...")

stop_event = threading.Event()


class SafeCounter:
    def __init__(self):
        self.sent = 0
        self.failed = 0
        self._lock = threading.Lock()

    def inc_sent(self):
        with self._lock:
            self.sent += 1

    def inc_failed(self):
        with self._lock:
            self.failed += 1

    def snapshot(self):
        with self._lock:
            return self.sent, self.failed


counter = SafeCounter()


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda v: json.dumps(v).encode(  # type: ignore
                    'utf-8'),
                linger_ms=100,        # aspetta max 100ms prima di inviare batch
                batch_size=16384,     # 16 KB per batch
                max_request_size=1048576,  # max 1 MB per richiesta
                acks=1                # meno overhead
            )
            print("Producer Kafka connesso.")
            return producer
        except Exception as e:
            print(f"Errore connessione Kafka: {e}. Riprovo in 1s...")
            time.sleep(1)


def send_temperature(thread_id: int):
    producer = create_producer()
    # count = 0

    while not stop_event.is_set():
        temperatura = 20 + random.randint(0, 10)
        data = {
            "temperatura": str(temperatura),
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "thread_id": str(thread_id)
        }

        try:
            producer.send("temperature", data) # type: ignore
            counter.inc_sent()
        except Exception:
            counter.inc_failed()
            producer.close() # type: ignore
            producer = create_producer()

        # count += 1
        # if count % 10 == 0:  # stampa ogni 10 messaggi
        #     sent, failed = counter.snapshot()
        #     print(f"[Thread {thread_id}] sent: {sent}, failed: {failed}")

        time.sleep(0.01)

    producer.close() # type: ignore



threads: list[threading.Thread] = []
for i in range(NUM_THREADS):
    t = threading.Thread(target=send_temperature, args=(i+1,), daemon=True)
    t.start()
    threads.append(t)  # type: ignore

print(f"Avviati {NUM_THREADS} thread. PID: {os.getpid()}")

try:
    stop_event.wait(timeout=600)
    stop_event.set()
    print("\nTempo scaduto: fermo tutti i thread...")
    for t in threads:
        t.join()

except KeyboardInterrupt:
    print("\nTerminazione richiesta dall'utente.")
    stop_event.set()
    for t in threads:
        t.join()

# Statistiche
sent, failed = counter.snapshot()
print("\n=== STATISTICHE ===")
print(f"Messaggi inviati: {sent}")
print(f"Messaggi falliti: {failed}")
print("===================")
