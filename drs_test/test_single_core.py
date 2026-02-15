from __future__ import annotations

import time
import random
import statistics
import logging
import multiprocessing as mp
from typing import List, TypedDict
from multiprocessing.synchronize import Event
from multiprocessing.queues import Queue
from drs import drs  # type: ignore


# ==========================================================
# CONFIGURAZIONE ESPERIMENTO
# ==========================================================

NUMBER_OF_TASKS: int = 4               # Numero totale di task periodici
TOTAL_CPU_UTILIZATION: float = 0.50       # Utilizzazione totale desiderata (somma C_i / T_i)
SIMULATION_DURATION_SECONDS: int = 60 * 1     # Durata dell’esperimento
LOG_PROGRESS_EVERY_N_JOBS: int = 50       # Frequenza log di avanzamento


# ==========================================================
# CONFIGURAZIONE LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(processName)s | %(message)s",
    datefmt="%H:%M:%S",
)


# ==========================================================
# STRUTTURA DATI RISULTATO TASK
# ==========================================================

class PeriodicTaskResult(TypedDict):
    task_id: int
    total_jobs_executed: int
    total_deadline_misses: int
    worst_case_response_time: float
    average_response_time: float


# ==========================================================
# FUNZIONE TASK PERIODICO (PROCESSO SEPARATO)
# ==========================================================

def periodic_real_time_task(
    task_id: int,
    execution_time_seconds: float,
    period_seconds: float,
    stop_signal: Event,
    result_queue: Queue[PeriodicTaskResult],
) -> None:
    """
    Simula un task periodico real-time.

    - execution_time_seconds (C): tempo di esecuzione simulato (busy wait)
    - period_seconds (T): periodo del task
    - stop_signal: evento per terminare l’esecuzione
    - result_queue: coda per inviare i risultati al processo principale
    """

    logging.info(
        f"Task {task_id} START | "
        f"C={execution_time_seconds:.6f}s | "
        f"T={period_seconds:.6f}s | "
        f"U={execution_time_seconds / period_seconds:.3f}"
    )

    next_release_time: float = time.perf_counter()
    total_deadline_misses: int = 0
    response_time_samples: List[float] = []
    total_jobs_executed: int = 0

    # Loop principale del task periodico
    while not stop_signal.is_set():

        job_start_time: float = time.perf_counter()

        # Simulazione esecuzione reale tramite busy-wait
        while (time.perf_counter() - job_start_time) < execution_time_seconds:
            pass

        job_finish_time: float = time.perf_counter()

        # Response time = tempo di completamento - release time atteso
        response_time: float = job_finish_time - next_release_time
        response_time_samples.append(response_time)

        # Verifica deadline miss
        if response_time > period_seconds:
            total_deadline_misses += 1
            logging.warning(
                f"Task {task_id} DEADLINE MISS | "
                f"RT={response_time:.6f}s"
            )

        total_jobs_executed += 1

        # Log di avanzamento
        if total_jobs_executed % LOG_PROGRESS_EVERY_N_JOBS == 0:
            logging.info(
                f"Task {task_id} progress | "
                f"jobs={total_jobs_executed} | "
                f"miss={total_deadline_misses}"
            )

        # Calcolo prossimo rilascio periodico
        next_release_time += period_seconds

        sleep_time: float = next_release_time - time.perf_counter()

        if sleep_time > 0:
            time.sleep(sleep_time)

    # Calcolo metriche finali del task
    result: PeriodicTaskResult = {
        "task_id": task_id,
        "total_jobs_executed": total_jobs_executed,
        "total_deadline_misses": total_deadline_misses,
        "worst_case_response_time": (
            max(response_time_samples) if response_time_samples else 0.0
        ),
        "average_response_time": (
            statistics.mean(response_time_samples)
            if response_time_samples else 0.0
        ),
    }

    logging.info(
        f"Task {task_id} END | "
        f"jobs={total_jobs_executed} | "
        f"miss={total_deadline_misses} | "
        f"WCRT={result['worst_case_response_time']:.6f}"
    )

    result_queue.put(result)


# ==========================================================
# MAIN - CONTROLLO ESPERIMENTO
# ==========================================================

def main() -> None:
    """
    Funzione principale:
    - Genera task set con DRS
    - Avvia processi multipli
    - Esegue simulazione per tempo prefissato
    - Raccoglie e aggrega risultati
    """

    logging.info("=== TEST START ===")

    # ------------------------------------------------------
    # Generazione utilizzi tramite Dirichlet-Rescale (DRS)
    # ------------------------------------------------------

    utilization_vector: List[float] = drs(
        n=NUMBER_OF_TASKS,
        sumu=TOTAL_CPU_UTILIZATION
    )

    # Generazione periodi casuali
    period_list_seconds: List[float] = [
        random.uniform(0.05, 0.15)
        for _ in range(NUMBER_OF_TASKS)
    ]

    # Calcolo execution time C_i = U_i * T_i
    execution_time_list_seconds: List[float] = [
        utilization_vector[i] * period_list_seconds[i]
        for i in range(NUMBER_OF_TASKS)
    ]

    logging.info("Generated task set.")

    # ------------------------------------------------------
    # Creazione processi
    # ------------------------------------------------------

    stop_signal: Event = mp.Event()
    result_queue: Queue[PeriodicTaskResult] = mp.Queue()
    process_list: List[mp.Process] = []

    for task_index in range(NUMBER_OF_TASKS):
        process: mp.Process = mp.Process(
            target=periodic_real_time_task,
            args=(
                task_index,
                execution_time_list_seconds[task_index],
                period_list_seconds[task_index],
                stop_signal,
                result_queue,
            ),
            name=f"TaskProcess-{task_index}",
        )
        process.start()
        process_list.append(process)

    logging.info("All tasks started.")

    # Esecuzione esperimento
    time.sleep(SIMULATION_DURATION_SECONDS)

    logging.info("Stopping tasks...")
    stop_signal.set()

    for process in process_list:
        process.join()

    logging.info("All tasks terminated.")

    # ------------------------------------------------------
    # Aggregazione risultati globali
    # ------------------------------------------------------

    collected_results: List[PeriodicTaskResult] = []

    while not result_queue.empty():
        collected_results.append(result_queue.get())

    total_jobs: int = sum(r["total_jobs_executed"]
                          for r in collected_results)

    total_deadline_misses: int = sum(
        r["total_deadline_misses"]
        for r in collected_results
    )

    global_worst_case_response_time: float = max(
        (r["worst_case_response_time"] for r in collected_results),
        default=0.0,
    )

    global_average_response_time: float = (
        statistics.mean(r["average_response_time"]
                        for r in collected_results)
        if collected_results else 0.0
    )

    # ------------------------------------------------------
    # Stampa Sommario Finale
    # ------------------------------------------------------

    logging.info("\n===== FINAL SUMMARY =====")
    logging.info(f"Number of tasks: {NUMBER_OF_TASKS}")
    logging.info(f"Total CPU utilization: {TOTAL_CPU_UTILIZATION}")
    logging.info(f"Total executed jobs: {total_jobs}")

    if total_deadline_misses > 0:
        logging.error(
            f"Total deadline misses: {total_deadline_misses}"
        )
    else:
        logging.info(
            f"Total deadline misses: {total_deadline_misses}"
        )

    logging.info(
        f"Miss ratio: "
        f"{(total_deadline_misses / total_jobs):.4f}"
        if total_jobs else "Miss ratio: 0.0"
    )

    logging.info(
        f"Global worst-case response time: "
        f"{global_worst_case_response_time:.6f} s"
    )

    logging.info(
        f"Global average response time: "
        f"{global_average_response_time:.6f} s"
    )

    logging.info("==========================")
    logging.info("=== TEST END ===")


if __name__ == "__main__":
    main()