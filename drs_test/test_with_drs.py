"""
This code is a test for the course RTK&S, where we evaluate the performance, in a real-time context,
of an embedded device's CPU scheduler. The drs library is used to generate unbiased utilization
vectors for a synthetic task set. Tasks are executed concurrently using multiprocessing to evaluate
deadline misses, worst-case response time, and jitter.
"""

from drs import drs  # type: ignore
import time
import statistics
from multiprocessing import Process, Manager

# -----------------------------
# Configuration parameters
# -----------------------------
N_TASKS = 1000
EXECUTION_PERIOD_TIME = 0.05        # 50 ms task period (implicit deadline)
N_CORES = 4                          # CPU cores available

RELATIVE_UTILIZATION = 0.50          # Target per-core utilization (85%)
DRS_UTILIZATION = N_CORES * RELATIVE_UTILIZATION  # Total utilization for all tasks

N_ITERATIONS = 100                   # Number of test repetitions

# Real-time thresholds
HARD_REAL_TIME_THRESHOLD = 0.0
SOFT_REAL_TIME_THRESHOLD = 0.0001

# -----------------------------
# Task function
# -----------------------------
def run_task(C: float, T: float, response_list:list[float], deadline_counter:list[int]) -> None:
    start_period = time.perf_counter()

    # Busy-wait per simulare il carico CPU
    while (time.perf_counter() - start_period) < C:
        pass

    # Resto del periodo
    elapsed = time.perf_counter() - start_period
    sleep_time = T - elapsed
    if sleep_time > 0:
        time.sleep(sleep_time)

    end_period = time.perf_counter()
    response_time = end_period - start_period

    # Aggiorna metriche condivise (multiprocessing safe)
    response_list.append(response_time)
    if response_time > T:
        deadline_counter.append(1)

# -----------------------------
# Test execution
# -----------------------------
manager = Manager()
response_times = manager.list()
deadline_misses_list = manager.list()

for _ in range(N_ITERATIONS):

    # 1) Generate utilization vector using DRS
    utilizations = drs(N_TASKS, DRS_UTILIZATION)

    # 2) Compute execution times Ci = Ui * Ti
    execution_times = [u * EXECUTION_PERIOD_TIME for u in utilizations]

    # 3) Launch all tasks concurrently as processes
    processes:list[Process] = []
    for C in execution_times:
        p = Process(target=run_task, args=(C, EXECUTION_PERIOD_TIME, response_times, deadline_misses_list))
        p.start()
        processes.append(p)

    # 4) Wait for all processes to finish
    for p in processes:
        p.join()

# -----------------------------
# Metrics computation
# -----------------------------
total_executions = N_TASKS * N_ITERATIONS
deadline_misses = len(deadline_misses_list)
deadline_miss_ratio = deadline_misses / total_executions
worst_case_response_time = max(response_times)
jitter = max(response_times) - min(response_times)
average_response_time = statistics.mean(response_times)

# -----------------------------
# Results
# -----------------------------
print("=== Real-Time Scheduler Test Results ===")
print(f"Total executions: {total_executions}")
print(f"Deadline misses: {deadline_misses}")
print(f"Deadline miss ratio: {deadline_miss_ratio:.6f}")
print(f"Worst-case response time: {worst_case_response_time:.6f} s")
print(f"Average response time: {average_response_time:.6f} s")
print(f"Jitter: {jitter:.6f} s")

# -----------------------------
# Real-time evaluation
# -----------------------------
if deadline_miss_ratio <= HARD_REAL_TIME_THRESHOLD:
    print("Result: ACCEPTABLE for hard real-time usage")
elif deadline_miss_ratio <= SOFT_REAL_TIME_THRESHOLD:
    print("Result: ACCEPTABLE for soft real-time usage")
else:
    print("Result: NOT ACCEPTABLE for soft or hard real-time usage")
