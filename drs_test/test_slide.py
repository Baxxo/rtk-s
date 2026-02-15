from __future__ import annotations

import os
import time
import random
import statistics
import logging
import multiprocessing as mp
from typing import List, TypedDict
from multiprocessing.synchronize import Event
from multiprocessing.queues import Queue
from drs import drs # type: ignore


# ==========================================================
# CONFIGURAZIONE ESPERIMENTO
# ==========================================================

NUMBER_OF_TASKS: int = 35
NUMBER_OF_CORE:int = 4
TOTAL_CPU_UTILIZATION: float = 0.6 * NUMBER_OF_CORE
SIMULATION_DURATION_SECONDS: int = 60
LOG_PROGRESS_EVERY_N_JOBS: int = 50


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
# TASK PERIODICO
# ==========================================================

def periodic_real_time_task(
    task_id: int,
    execution_time_seconds: float,
    period_seconds: float,
    stop_signal: Event,
    result_queue: Queue[PeriodicTaskResult],
    core_id: int,
) -> None:

    # Pin al core
    os.sched_setaffinity(0, {core_id}) # type: ignore

    logging.info(
        f"Task {task_id} pinned to core {core_id} | "
        f"C={execution_time_seconds:.6f}s | "
        f"T={period_seconds:.6f}s | "
        f"U={execution_time_seconds / period_seconds:.3f}"
    )

    next_release_time: float = time.perf_counter()
    total_deadline_misses: int = 0
    response_time_samples: List[float] = []
    total_jobs_executed: int = 0

    while not stop_signal.is_set():

        job_start_time: float = time.perf_counter()

        while (time.perf_counter() - job_start_time) < execution_time_seconds:
            pass

        job_finish_time: float = time.perf_counter()

        response_time: float = job_finish_time - next_release_time
        response_time_samples.append(response_time)

        if response_time > period_seconds:
            total_deadline_misses += 1
            logging.warning(
                f"Task {task_id} DEADLINE MISS | RT={response_time:.6f}s"
            )

        total_jobs_executed += 1

        if total_jobs_executed % LOG_PROGRESS_EVERY_N_JOBS == 0:
            logging.info(
                f"Task {task_id} progress | "
                f"jobs={total_jobs_executed} | "
                f"miss={total_deadline_misses}"
            )

        next_release_time += period_seconds
        sleep_time: float = next_release_time - time.perf_counter()

        if sleep_time > 0:
            time.sleep(sleep_time)

    result: PeriodicTaskResult = {
        "task_id": task_id,
        "total_jobs_executed": total_jobs_executed,
        "total_deadline_misses": total_deadline_misses,
        "worst_case_response_time": max(response_time_samples, default=0.0),
        "average_response_time": (
            statistics.mean(response_time_samples)
            if response_time_samples else 0.0
        ),
    }

    logging.info(
        f"Task {task_id} END | "
        f"jobs={total_jobs_executed} | "
        f"miss={total_deadline_misses}"
    )

    result_queue.put(result)


# ==========================================================
# MAIN
# ==========================================================

def main() -> None:

    logging.info("=== TEST START ===")

    worst_case: List[float] = drs(
        n=NUMBER_OF_TASKS,
        sumu=random.uniform(0.7, 0.9) * NUMBER_OF_CORE,
    )


    utilization_vector: List[float] = drs(
        n=NUMBER_OF_TASKS,
        sumu=TOTAL_CPU_UTILIZATION,
        upper_bounds=worst_case
    )

    period_list_seconds: List[float] = [
        random.uniform(0.01, 1)
        for _ in range(NUMBER_OF_TASKS)
    ]

    execution_time_list_seconds: List[float] = [
        utilization_vector[i] * period_list_seconds[i]
        for i in range(NUMBER_OF_TASKS)
    ]

    available_cores: List[int] = sorted(os.sched_getaffinity(0)) # type: ignore
    num_cores: int = len(available_cores)

    logging.info(f"Detected CPU cores: {available_cores}")

    stop_signal: Event = mp.Event()
    result_queue: Queue[PeriodicTaskResult] = mp.Queue()
    process_list: List[mp.Process] = []

    # Round-robin assignment
    for task_index in range(NUMBER_OF_TASKS):

        assigned_core: int = available_cores[
            task_index % num_cores
        ]

        logging.info(
            f"Assigning Task {task_index} to core {assigned_core}"
        )

        process: mp.Process = mp.Process(
            target=periodic_real_time_task,
            args=(
                task_index,
                execution_time_list_seconds[task_index],
                period_list_seconds[task_index],
                stop_signal,
                result_queue,
                assigned_core,
            ),
            name=f"TaskProcess-{task_index}",
        )

        process.start()
        process_list.append(process)

    logging.info("All tasks started.")

    time.sleep(SIMULATION_DURATION_SECONDS)

    logging.info("Stopping tasks...")
    stop_signal.set()

    for process in process_list:
        process.join()

    logging.info("All tasks terminated.")

    # Aggregazione risultati
    collected_results: List[PeriodicTaskResult] = []

    while not result_queue.empty():
        collected_results.append(result_queue.get())

    total_jobs: int = sum(r["total_jobs_executed"] for r in collected_results)
    total_deadline_misses: int = sum(r["total_deadline_misses"] for r in collected_results)

    global_wcrt: float = max(
        (r["worst_case_response_time"] for r in collected_results),
        default=0.0,
    )

    global_avg_rt: float = (
        statistics.mean(r["average_response_time"] for r in collected_results)
        if collected_results else 0.0
    )

    logging.info("\n===== FINAL SUMMARY =====")
    logging.info(f"Tasks: {NUMBER_OF_TASKS}")
    logging.info(f"Total Utilization: {TOTAL_CPU_UTILIZATION}")
    logging.info(f"Total Jobs: {total_jobs}")
    logging.info(f"Deadline Misses: {total_deadline_misses}")
    logging.info(
        f"Miss Ratio: "
        f"{(total_deadline_misses / total_jobs):.4f}"
        if total_jobs else "0.0"
    )
    logging.info(f"Global WCRT: {global_wcrt:.6f}s")
    logging.info(f"Global Avg RT: {global_avg_rt:.6f}s")
    logging.info("==========================")
    logging.info("=== TEST END ===")


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    main()
