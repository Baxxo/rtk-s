"""
This code is a test for the course RTK&S, where we evaluate the performance, in a real-time context,
of an embedded device's CPU scheduler. The drs library is used to generate unbiased utilization
vectors for a synthetic task set. Tasks are then executed in order to evaluate deadline misses,
worst-case response time, and jitter.
"""

from drs import drs # type: ignore
import time
import statistics

# -----------------------------
# Configuration parameters
# -----------------------------

N_TASKS = 10

EXECUTION_PERIOD_TIME = 0.05        # 50 ms task period (implicit deadline)

N_CORES = 4                  # CPU cores available

# RELATIVE_UTILIZATION = 0.95  # 95% CPU utilization target for DRS
# RELATIVE_UTILIZATION = 0.50  # 50% CPU utilization target for DRS
RELATIVE_UTILIZATION = 0.25  # 25% CPU utilization target for DRS

DRS_UTILIZATION = N_CORES * RELATIVE_UTILIZATION


N_ITERATIONS = 100                  # number of test repetitions

# -----------------------------
# Test execution
# -----------------------------
deadline_misses = 0
response_times:list[float] = []

for _ in range(N_ITERATIONS):

    # 1) Generate utilization vector using DRS
    utilizations = drs(N_TASKS, DRS_UTILIZATION)

    # 2) Compute execution times Ci = Ui * Ti
    execution_times = [u * EXECUTION_PERIOD_TIME for u in utilizations]

    # 3) Simulate task execution
    for C in execution_times:
        start = time.perf_counter()
        time.sleep(C)               # workload simulation
        end = time.perf_counter()

        response_time = end - start
        response_times.append(response_time)

        # 4) Deadline check (implicit deadline: D = T)
        if response_time > EXECUTION_PERIOD_TIME:
            deadline_misses += 1

# -----------------------------
# Metrics computation
# -----------------------------
total_executions = N_TASKS * N_ITERATIONS

deadline_miss_ratio = deadline_misses / total_executions
worst_case_response_time = max(response_times)
jitter = max(response_times) - min(response_times)
average_response_time = statistics.mean(response_times)

# -----------------------------
# Results
# -----------------------------
print("=== Real-Time Scheduler Test Results ===")
print(f"Total executions: {total_executions}")
print(f"Deadline miss ratio: {deadline_miss_ratio:.4f}")
print(f"Worst-case response time: {worst_case_response_time:.6f} s")
print(f"Average response time: {average_response_time:.6f} s")
print(f"Jitter: {jitter:.6f} s")


HARD_REAL_TIME_THRESHOLD = 0.0        # 0% deadline miss ratio (hard real-time threshold)
SOFT_REAL_TIME_THRESHOLD = 0.0001    # 0.0001% deadline miss ratio (soft real-time threshold)


if deadline_miss_ratio <= SOFT_REAL_TIME_THRESHOLD:
    print("Result: ACCEPTABLE for soft real-time usage")

if deadline_miss_ratio <= HARD_REAL_TIME_THRESHOLD:
    print("Result: ACCEPTABLE for hard real-time usage")

if deadline_miss_ratio > SOFT_REAL_TIME_THRESHOLD and deadline_miss_ratio > HARD_REAL_TIME_THRESHOLD:
    print("Result: NOT ACCEPTABLE for soft real-time usage and NOT ACCEPTABLE for hard real-time usage")
