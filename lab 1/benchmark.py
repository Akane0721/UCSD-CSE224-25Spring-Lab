import subprocess
import numpy as np
import matplotlib.pyplot as plt
import os

GENSORT = "./bin/gensort"
SORT = "./bin/sort"
TEMP_INPUT = "temp_input.dat"
TEMP_OUTPUT = "temp_output.dat"

def measure_avg_time(REPEAT=10):
    command = f"for i in {{1..{REPEAT}}}; do {SORT} {TEMP_INPUT} {TEMP_OUTPUT}; done"

    result = subprocess.run(
        ["/usr/bin/time", "-p", "bash", "-c", command],
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        text=True
    )

    lines = result.stderr.strip().split("\n")
    for line in lines:
        if line.startswith("real"):
            total_time = float(line.split()[1])
            avg_time = total_time / REPEAT

    return avg_time

sizes = np.logspace(3, 7, 20, dtype=int)
runtimes = []

for size in sizes:
    size_str = str(size)
    subprocess.run([GENSORT, size_str, TEMP_INPUT], check=True)
    average_time = measure_avg_time(REPEAT=100)
    # result = subprocess.run(["/usr/bin/time", "-p", SORT, TEMP_INPUT, TEMP_OUTPUT],
    #                         stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
    # output = result.stderr.strip()
    # for line in output.splitlines():
    #     if line.startswith("real"):
    #         average_time = float(line.split()[1])
    print(f"Runtime for input size {size_str} is {average_time} s")
    runtimes.append(average_time)

# Clean up
os.remove(TEMP_INPUT)
os.remove(TEMP_OUTPUT)

# Plotting
plt.figure(figsize=(10, 6))
plt.loglog(sizes, runtimes, label="Observed Runtime", marker="o")

# Asymptotic bound: assume O(n log n)
nlogn = [n * np.log2(n) for n in sizes]
# Normalize for visual comparison
scaling_factor = runtimes[-1] / nlogn[-1]
nlogn_scaled = [y * scaling_factor for y in nlogn]
plt.loglog(sizes, nlogn_scaled, label="O(n log n) bound", linestyle="--")

# Labels and legends
plt.xlabel("Input Size (bytes)")
plt.ylabel("Time (seconds)")
plt.title("Sort Runtime vs Input Size")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("runtime_plot.png")
plt.show()