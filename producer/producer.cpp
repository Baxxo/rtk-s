#include <rdkafka/rdkafka.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

static const std::string KAFKA_SERVER = "192.168.178.166:29092";
static const std::string TOPIC = "temperature";

std::atomic<bool> stop_event(false);

/* =======================
   SafeCounter
   ======================= */
class SafeCounter {
private:
    int sent{0};
    int failed{0};
    std::mutex mtx;

public:
    void inc_sent() {
        std::lock_guard<std::mutex> lock(mtx);
        sent++;
    }

    void inc_failed() {
        std::lock_guard<std::mutex> lock(mtx);
        failed++;
    }

    std::pair<int, int> snapshot() {
        std::lock_guard<std::mutex> lock(mtx);
        return {sent, failed};
    }
};

SafeCounter counter;

/* =======================
   Utility timestamp ISO
   ======================= */
std::string now_iso() {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  now.time_since_epoch())
                  .count() %
              1000000;

    std::ostringstream oss;
    oss << std::put_time(std::localtime(&t), "%Y-%m-%dT%H:%M:%S")
        << "." << std::setw(6) << std::setfill('0') << us;
    return oss.str();
}

/* =======================
   Create Kafka producer
   ======================= */
rd_kafka_t* create_producer() {
    char errstr[512];

    while (true) {
        rd_kafka_conf_t* conf = rd_kafka_conf_new();

        if (rd_kafka_conf_set(conf, "bootstrap.servers",
                              KAFKA_SERVER.c_str(),
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::cerr << "Errore config Kafka: " << errstr << std::endl;
            rd_kafka_conf_destroy(conf);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        rd_kafka_t* producer = rd_kafka_new(
            RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));

        if (!producer) {
            std::cerr << "Errore connessione Kafka: "
                      << errstr << ". Riprovo...\n";
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        std::cout << "Producer Kafka connesso.\n";
        return producer;
    }
}

/* =======================
   Thread worker
   ======================= */
void send_temperature(int thread_id) {
    rd_kafka_t* producer = create_producer();
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(20, 30);

    while (!stop_event.load()) {
        int temperatura = dist(rng);

        std::ostringstream json;
        json << "{"
             << "\"temperatura\":" << temperatura << ","
             << "\"timestamp\":\"" << now_iso() << "\","
             << "\"thread_id\":" << thread_id
             << "}";

        std::string payload = json.str();

        rd_kafka_resp_err_t err = rd_kafka_producev(
            producer,
            RD_KAFKA_V_TOPIC(TOPIC.c_str()),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE(payload.c_str(), payload.size()),
            RD_KAFKA_V_END);

        if (err) {
            counter.inc_failed();
            std::cerr << "[Thread " << thread_id
                      << "] Errore invio: "
                      << rd_kafka_err2str(err)
                      << ". Riconnessione...\n";

            rd_kafka_flush(producer, 5000);
            rd_kafka_destroy(producer);
            producer = create_producer();
        } else {
            rd_kafka_flush(producer, 1000);
            counter.inc_sent();
            std::cout << "[Thread " << thread_id
                      << "] Inviato: " << payload << "\n";
        }

        auto [sent, failed] = counter.snapshot();
        std::cout << "[Thread " << thread_id
                  << "] sent: " << sent
                  << ", failed: " << failed << "\n";

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    rd_kafka_flush(producer, 5000);
    rd_kafka_destroy(producer);
    std::cout << "[Thread " << thread_id << "] Terminato.\n";
}

/* =======================
   Signal handler
   ======================= */
void handle_signal(int) {
    stop_event.store(true);
}

/* =======================
   MAIN
   ======================= */
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Uso: ./producer <NUM_THREADS>\n";
        return 1;
    }

    int NUM_THREADS = std::stoi(argv[1]);
    std::cout << "Avvio di " << NUM_THREADS << " thread...\n";

    signal(SIGINT, handle_signal);

    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back(send_temperature, i + 1);
    }

    std::this_thread::sleep_for(std::chrono::seconds(600));
    stop_event.store(true);

    for (auto& t : threads)
        t.join();

    auto [sent, failed] = counter.snapshot();
    std::cout << "\n=== STATISTICHE ===\n";
    std::cout << "Messaggi inviati: " << sent << "\n";
    std::cout << "Messaggi falliti: " << failed << "\n";
    std::cout << "===================\n";

    return 0;
}
