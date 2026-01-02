#include <rdkafka/rdkafka.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>

static const char* KAFKA_SERVER = "192.168.178.166:29092";
static const char* TOPIC = "temperature";

std::atomic<bool> stop_flag(false);
std::mutex counter_mtx;
int sent = 0, failed = 0;

void signal_handler(int) {
    stop_flag.store(true);
}

rd_kafka_t* create_producer() {
    char errstr[512];

    while (!stop_flag.load()) {
        rd_kafka_conf_t* conf = rd_kafka_conf_new();

        rd_kafka_conf_set(conf, "bootstrap.servers",
                          KAFKA_SERVER, nullptr, 0);

        rd_kafka_t* producer =
            rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));

        if (!producer) {
            std::cerr << "Kafka error: " << errstr << "\n";
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        return producer;
    }
    return nullptr;
}

void worker(int id) {
    rd_kafka_t* producer = create_producer();
    if (!producer) return;

    std::mt19937 rng(id);
    std::uniform_int_distribution<int> dist(20, 30);

    while (!stop_flag.load()) {
        int temp = dist(rng);

        std::ostringstream json;
        json << "{"
             << "\"temperatura\":" << temp << ","
             << "\"thread_id\":" << id
             << "}";

        std::string payload = json.str();

        if (rd_kafka_producev(
                producer,
                RD_KAFKA_V_TOPIC(TOPIC),
                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                RD_KAFKA_V_VALUE(payload.c_str(), payload.size()),
                RD_KAFKA_V_END) != RD_KAFKA_RESP_ERR_NO_ERROR) {

            std::lock_guard<std::mutex> lock(counter_mtx);
            failed++;
        } else {
            std::lock_guard<std::mutex> lock(counter_mtx);
            sent++;
        }

        rd_kafka_poll(producer, 0);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    rd_kafka_flush(producer, 3000);
    rd_kafka_destroy(producer);
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Uso: producer <threads>\n";
        return 1;
    }

    signal(SIGINT, signal_handler);

    int threads = atoi(argv[1]);
    for (int i = 0; i < threads; ++i)
        std::thread(worker, i + 1).detach();

    std::this_thread::sleep_for(std::chrono::minutes(10));
    stop_flag.store(true);

    std::cout << "Sent: " << sent
              << " Failed: " << failed << "\n";
}
