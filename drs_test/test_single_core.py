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


# ------------------------
# CONFIGURAZIONE
# ------------------------

N_TASKS: int = 4
TOTAL_UTIL: float = 0.9
SIMULATION_TIME: int = 10
LOG_EVERY_N_JOBS: int = 50


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(processName)s | %(message)s",
    datefmt="%H:%M:%S",
)


# ------------------------
# STRUTTURA RISULTATI
# ------------------------

class TaskResult(TypedDict):
    task_id: int
    jobs: int
    miss: int
    max_rt: float
    avg_rt: float


# ------------------------
# TASK PERIODICO
# ------------------------

def periodic_task(
    task_id: int,
    C: float,
    T: float,
    stop_event: Event,
    result_queue: Queue[TaskResult],
) -> None:

    logging.info(
        f"Task {task_id} START | C={C:.6f}s | T={T:.6f}s | U={C/T:.3f}"
    )

    next_release: float = time.perf_counter()
    deadline_miss: int = 0
    response_times: List[float] = []
    jobs: int = 0

    while not stop_event.is_set():

        start: float = time.perf_counter()

        # Busy wait
        while (time.perf_counter() - start) < C:
            pass

        finish: float = time.perf_counter()
        response_time: float = finish - next_release
        response_times.append(response_time)

        if response_time > T:
            deadline_miss += 1
            logging.warning(
                f"Task {task_id} DEADLINE MISS | RT={response_time:.6f}s"
            )

        jobs += 1

        if jobs % LOG_EVERY_N_JOBS == 0:
            logging.info(
                f"Task {task_id} progress | jobs={jobs} | "
                f"miss={deadline_miss}"
            )

        next_release += T
        sleep_time: float = next_release - time.perf_counter()

        if sleep_time > 0:
            time.sleep(sleep_time)

    result: TaskResult = {
        "task_id": task_id,
        "jobs": jobs,
        "miss": deadline_miss,
        "max_rt": max(response_times) if response_times else 0.0,
        "avg_rt": statistics.mean(response_times) if response_times else 0.0,
    }

    logging.info(
        f"Task {task_id} END | jobs={jobs} | miss={deadline_miss} | "
        f"maxRT={result['max_rt']:.6f}"
    )

    result_queue.put(result)


# ------------------------
# MAIN
# ------------------------

def main() -> None:

    logging.info("=== TEST START ===")

    u: List[float] = drs(n=N_TASKS, sumu=TOTAL_UTIL)

    T: List[float] = [random.uniform(0.05, 0.15) for _ in range(N_TASKS)]
    C: List[float] = [u[i] * T[i] for i in range(N_TASKS)]

    logging.info("Generated task set:")
    for i in range(N_TASKS):
        logging.info(
            f"Task {i} | U={u[i]:.3f} | T={T[i]:.6f} | C={C[i]:.6f}"
        )

    stop_event: Event = mp.Event()
    result_queue: Queue[TaskResult] = mp.Queue()
    processes: List[mp.Process] = []

    for i in range(N_TASKS):
        p: mp.Process = mp.Process(
            target=periodic_task,
            args=(i, C[i], T[i], stop_event, result_queue),
            name=f"TaskProcess-{i}",
        )
        p.start()
        processes.append(p)

    logging.info("All tasks started.")
    time.sleep(SIMULATION_TIME)

    logging.info("Stopping tasks...")
    stop_event.set()

    for p in processes:
        p.join()

    logging.info("All tasks terminated.")

    # ------------------------
    # SOMMARIO FINALE
    # ------------------------

    results: List[TaskResult] = []

    while not result_queue.empty():
        results.append(result_queue.get())

    total_jobs: int = sum(r["jobs"] for r in results)
    total_miss: int = sum(r["miss"] for r in results)
    max_wcrt: float = max((r["max_rt"] for r in results), default=0.0)
    avg_wcrt: float = (
        statistics.mean(r["avg_rt"] for r in results)
        if results else 0.0
    )

    print("\n===== RISULTATI FINALI =====")
    print(f"Task totali: {N_TASKS}")
    print(f"Utilizzazione totale: {TOTAL_UTIL}")
    print(f"Job eseguiti: {total_jobs}")
    print(f"Deadline miss totali: {total_miss}")
    print(
        f"Miss ratio: {total_miss / total_jobs:.4f}" if total_jobs else "Miss ratio: 0.0")
    print(f"Worst-case response time globale: {max_wcrt:.6f} s")
    print(f"Average response time medio: {avg_wcrt:.6f} s")
    print("============================")

    logging.info("=== TEST END ===")


if __name__ == "__main__":
    main()
