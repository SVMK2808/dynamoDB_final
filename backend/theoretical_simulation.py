#!/usr/bin/env python3
import numpy as np
import matplotlib.pyplot as plt
import time
from tqdm import tqdm

# Simulation Constants
CLUSTER_SIZE = 1000
NUM_KEYS = 1_000_000  # Total keys to insert
BATCH_SIZE = 1000     # Process in batches for memory efficiency

# Latency Model (in milliseconds)
# We assume a large cluster where most requests involve network hops
NETWORK_MEAN = 20.0
NETWORK_STD = 5.0

# Disk Latency: Writes are faster (append-only), Reads are slower (seek)
WRITE_DISK_MEAN = 2.0
WRITE_DISK_STD = 0.5
READ_DISK_MEAN = 8.0
READ_DISK_STD = 2.0

# Reconciliation Overhead: Small CPU cost per extra replica involved in a read
# Reduced from 0.5ms to 0.05ms (50us) to be more realistic for CPU-bound tasks
READ_RECONCILIATION_BASE = 0.05 

# Fan-out Overhead: Cost to serialize and send requests to N nodes
# 0.01ms (10us) per node. For N=1000, this adds 10ms of processing time.
FANOUT_COST_PER_NODE = 0.01

# Failure Constants
TIMEOUT_MS = 1000.0       # 1 second timeout if quorum cannot be reached
CLIENT_RETRY_DELAY = 50.0 # 50ms penalty if coordinator fails and client retries

def simulate_latency(n, quorum_size, op_type="write", num_ops=1000, node_failure_rate=0.0, coord_failure_rate=0.0):
    """
    Simulates the latency of an operation that requires 'quorum_size' acks from 'n' replicas.
    Returns the average latency over num_ops.
    """
    # Generate latencies for all N replicas across all trials at once
    # Shape: (num_ops, n)
    
    # 1. Network RTT (One way * 2)
    net_delays = np.random.normal(NETWORK_MEAN, NETWORK_STD, (num_ops, n))
    # Ensure no negative latencies
    net_delays = np.maximum(net_delays, 1.0)
    
    # 2. Disk Processing
    if op_type == "write":
        disk_mean, disk_std = WRITE_DISK_MEAN, WRITE_DISK_STD
    else:
        disk_mean, disk_std = READ_DISK_MEAN, READ_DISK_STD

    disk_delays = np.random.normal(disk_mean, disk_std, (num_ops, n))
    disk_delays = np.maximum(disk_delays, 0.5)
    
    # Total latency for each replica
    total_delays = net_delays + disk_delays

    # 3. Simulate Node Failures
    if node_failure_rate > 0.0:
        # Generate a mask where True indicates a node failure
        # We use a random generator for this
        is_failed = np.random.random((num_ops, n)) < node_failure_rate
        # Failed nodes do not respond, effectively causing a timeout for that specific path
        total_delays[is_failed] = TIMEOUT_MS
    
    # Sort along the replica axis to find the k-th fastest response
    # We want the time when the 'quorum_size'-th replica responds
    total_delays.sort(axis=1)
    
    # The latency of the operation is determined by the slowest of the required quorum
    # Index is quorum_size - 1 because 0-indexed
    op_latencies = total_delays[:, quorum_size - 1]
    
    # Add Fan-out overhead (sending requests takes time)
    fanout_overhead = n * FANOUT_COST_PER_NODE
    op_latencies += fanout_overhead

    # Add reconciliation overhead for reads if quorum > 1
    if op_type == "read" and quorum_size > 1:
        # Overhead increases slightly with the number of replicas to reconcile
        overhead = (quorum_size - 1) * READ_RECONCILIATION_BASE
        op_latencies += overhead

    # 4. Simulate Coordinator Failures
    if coord_failure_rate > 0.0:
        # If coordinator fails, client must retry with another node
        # This adds a fixed retry penalty to the operation
        coord_failed = np.random.random(num_ops) < coord_failure_rate
        op_latencies[coord_failed] += CLIENT_RETRY_DELAY

    # 5. Simulate Read Repair (Only for Reads)
    # If nodes are failing/flaky, inconsistencies are likely.
    # A read might trigger a read-repair (write back to stale nodes) before returning (Read-Your-Writes)
    if op_type == "read" and node_failure_rate > 0.0:
        # Probability of needing repair scales with failure rate
        # If 20% nodes are failing, we assume 20% of reads might encounter stale/missing data requiring fix
        needs_repair = np.random.random(num_ops) < node_failure_rate
        
        # Repair cost: Network RTT + Disk Write (to update the stale node)
        repair_cost = NETWORK_MEAN + WRITE_DISK_MEAN
        op_latencies[needs_repair] += repair_cost

    return np.mean(op_latencies)

def run_million_keys_simulation():
    print("\n🚀 Starting Million-Key Insertion Simulation")
    print(f"   Cluster Size: {CLUSTER_SIZE} nodes")
    print(f"   Total Keys:   {NUM_KEYS:,}")
    print(f"   Model:        Network ~ N({NETWORK_MEAN}ms, {NETWORK_STD}ms)")
    
    # Define configurations to test
    configs = []
    
    # 1. Comprehensive small-scale tests (N=1 to 5)
    # We test ALL quorum sizes for these
    for n in [1, 2, 3, 4, 5]:
        for w in range(1, n + 1):
            configs.append({"N": n, "W": w, "label": f"N={n}, W={w}"})

    # 2. Large-scale tests (N=10 to 1000)
    # We test Weak (1), Majority (N/2 + 1), and Strong (N)
    for n in [10, 20, 50, 100, 200, 500, 1000]:
        w_majority = (n // 2) + 1
        configs.append({"N": n, "W": 1, "label": f"N={n}, W=1"})
        configs.append({"N": n, "W": w_majority, "label": f"N={n}, W={w_majority}"})
        configs.append({"N": n, "W": n, "label": f"N={n}, W={n}"})
    
    results = []
    
    for cfg in configs:
        n = cfg["N"]
        w = cfg["W"]
        label = cfg["label"]
        
        print(f"\nTesting Configuration: {label}")
        
        # We simulate the latency for the entire batch of 1 million keys
        # Since simulating 1M individual random variables is heavy, we do it in chunks
        # or just simulate the average latency and extrapolate.
        # For accuracy, let's simulate a representative sample and extrapolate.
        
        sample_size = 10000
        avg_latency_ms = simulate_latency(n, w, op_type="write", num_ops=sample_size)
        
        # Throughput calculation
        # If we have infinite concurrency, throughput is limited by hardware.
        # If we have a single client, throughput = 1000 / latency.
        # Let's assume a realistic concurrency of 100 workers.
        concurrency = 100
        ops_per_sec = (1000 / avg_latency_ms) * concurrency
        
        total_time_sec = NUM_KEYS / ops_per_sec
        
        print(f"   Average Latency: {avg_latency_ms:.2f} ms")
        print(f"   Est. Throughput: {ops_per_sec:.2f} ops/sec")
        print(f"   Est. Total Time: {total_time_sec:.2f} seconds ({total_time_sec/60:.2f} minutes)")
        
        results.append({
            "label": label,
            "throughput": ops_per_sec,
            "total_time": total_time_sec
        })

    # Plotting
    labels = [r["label"] for r in results]
    times = [r["total_time"] for r in results]
    
    plt.figure(figsize=(20, 10))  # Increased figure size for more labels
    # Changed to line graph as requested
    plt.plot(labels, times, marker='o', linestyle='-', color='#4682B4', linewidth=2, markersize=6)
    
    plt.title(f'Time to Insert {NUM_KEYS:,} Keys (Cluster Size={CLUSTER_SIZE})')
    plt.ylabel('Total Time (Seconds)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=90, ha='center', fontsize=8)  # Rotate labels 90 deg for density
    
    # Add value labels only for selected points to avoid clutter
    for i, time_val in enumerate(times):
        # Label every 3rd point or if it's an extreme value
        if i % 3 == 0 or i == len(times) - 1:
            plt.annotate(f'{time_val:.1f}s',
                        xy=(labels[i], time_val),
                        xytext=(0, 10),  # 10 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom',
                        fontsize=8,
                        fontweight='bold')
    
    plt.tight_layout()  # Adjust layout to prevent clipping
    filename = "graph_million_keys_time.png"
    plt.savefig(filename)
    print(f"\n✅ Graph saved to {filename}")

    # Plotting Throughput
    throughputs = [r["throughput"] for r in results]
    
    plt.figure(figsize=(20, 10))
    plt.plot(labels, throughputs, marker='o', linestyle='-', color='#2ECC71', linewidth=2, markersize=6)
    
    plt.title(f'Write Throughput (Cluster Size={CLUSTER_SIZE})')
    plt.ylabel('Throughput (Ops/Sec)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=90, ha='center', fontsize=8)
    
    for i, val in enumerate(throughputs):
        if i % 3 == 0 or i == len(throughputs) - 1:
            plt.annotate(f'{int(val)}',
                        xy=(labels[i], val),
                        xytext=(0, 10),
                        textcoords="offset points",
                        ha='center', va='bottom',
                        fontsize=8,
                        fontweight='bold')
    
    plt.tight_layout()
    plt.savefig("graph_million_keys_throughput.png")
    print("✅ Write Throughput Graph saved to graph_million_keys_throughput.png")

def run_read_simulation():
    print("\n🚀 Starting Million-Key Read Simulation")
    print(f"   Cluster Size: {CLUSTER_SIZE} nodes")
    print(f"   Total Keys:   {NUM_KEYS:,}")
    
    # Define configurations to test
    configs = []
    
    # 1. Comprehensive small-scale tests (N=1 to 5)
    for n in [1, 2, 3, 4, 5]:
        for r in range(1, n + 1):
            configs.append({"N": n, "R": r, "label": f"N={n}, R={r}"})

    # 2. Large-scale tests (N=10 to 1000)
    for n in [10, 20, 50, 100, 200, 500, 1000]:
        r_majority = (n // 2) + 1
        configs.append({"N": n, "R": 1, "label": f"N={n}, R=1"})
        configs.append({"N": n, "R": r_majority, "label": f"N={n}, R={r_majority}"})
        configs.append({"N": n, "R": n, "label": f"N={n}, R={n}"})
    
    results = []
    
    for cfg in configs:
        n = cfg["N"]
        r = cfg["R"]
        label = cfg["label"]
        
        # We use the same simulate_latency function because the logic 
        # (waiting for K responses from N nodes) is identical for R and W in this model.
        sample_size = 10000
        avg_latency_ms = simulate_latency(n, r, op_type="read", num_ops=sample_size)
        
        concurrency = 100
        ops_per_sec = (1000 / avg_latency_ms) * concurrency
        total_time_sec = NUM_KEYS / ops_per_sec
        
        results.append({
            "label": label,
            "throughput": ops_per_sec,
            "total_time": total_time_sec
        })

    # Plotting
    labels = [r["label"] for r in results]
    times = [r["total_time"] for r in results]
    
    plt.figure(figsize=(20, 10))
    # Using a different color (Orange) for Read operations
    plt.plot(labels, times, marker='o', linestyle='-', color='#E67E22', linewidth=2, markersize=6)
    
    plt.title(f'Time to Read {NUM_KEYS:,} Keys (Cluster Size={CLUSTER_SIZE})')
    plt.ylabel('Total Time (Seconds)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=90, ha='center', fontsize=8)
    
    for i, time_val in enumerate(times):
        if i % 3 == 0 or i == len(times) - 1:
            plt.annotate(f'{time_val:.1f}s',
                        xy=(labels[i], time_val),
                        xytext=(0, 10),
                        textcoords="offset points",
                        ha='center', va='bottom',
                        fontsize=8,
                        fontweight='bold')
    
    plt.tight_layout()
    filename = "graph_million_reads_time.png"
    plt.savefig(filename)
    print(f"\n✅ Read Graph saved to {filename}")

    # Plotting Throughput
    throughputs = [r["throughput"] for r in results]
    
    plt.figure(figsize=(20, 10))
    plt.plot(labels, throughputs, marker='o', linestyle='-', color='#9B59B6', linewidth=2, markersize=6)
    
    plt.title(f'Read Throughput (Cluster Size={CLUSTER_SIZE})')
    plt.ylabel('Throughput (Ops/Sec)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=90, ha='center', fontsize=8)
    
    for i, val in enumerate(throughputs):
        if i % 3 == 0 or i == len(throughputs) - 1:
            plt.annotate(f'{int(val)}',
                        xy=(labels[i], val),
                        xytext=(0, 10),
                        textcoords="offset points",
                        ha='center', va='bottom',
                        fontsize=8,
                        fontweight='bold')
    
    plt.tight_layout()
    plt.savefig("graph_million_reads_throughput.png")
    print("✅ Read Throughput Graph saved to graph_million_reads_throughput.png")

def run_failure_impact_simulation():
    n_values = [20, 50, 100]
    failure_rates = [0.0, 0.05, 0.10, 0.20] # 0%, 5%, 10%, 20% node failure
    
    for n in n_values:
        print(f"\n🚀 Starting Failure Impact Simulation (N={n})")
        print(f"   Cluster Size: {CLUSTER_SIZE} nodes")
        
        # Define W and R values: 1, ~25%, Majority, ~75%, All
        # We use a set to avoid duplicates and sorted to keep order
        w_values = sorted(list(set([1, int(n*0.25), (n//2)+1, int(n*0.75), n])))
        # Filter out 0 or invalid values just in case
        w_values = [x for x in w_values if x > 0]
        
        # 1. Write Simulation
        write_throughput_results = []
        write_latency_results = []
        print(f"   --- Simulating Writes (N={n}) ---")
        for w in w_values:
            w_throughput_series = []
            w_latency_series = []
            for fr in failure_rates:
                avg_latency = simulate_latency(n, w, op_type="write", num_ops=2000, 
                                             node_failure_rate=fr, coord_failure_rate=0.01)
                throughput = (1000 / avg_latency) * 100
                w_throughput_series.append(throughput)
                w_latency_series.append(avg_latency)
            write_throughput_results.append({"label": f"W={w}", "data": w_throughput_series})
            write_latency_results.append({"label": f"W={w}", "data": w_latency_series})

        # 2. Read Simulation
        # Use same values for R
        r_values = w_values
        read_throughput_results = []
        read_latency_results = []
        print(f"   --- Simulating Reads (N={n}) ---")
        for r in r_values:
            r_throughput_series = []
            r_latency_series = []
            for fr in failure_rates:
                avg_latency = simulate_latency(n, r, op_type="read", num_ops=2000, 
                                             node_failure_rate=fr, coord_failure_rate=0.01)
                throughput = (1000 / avg_latency) * 100
                r_throughput_series.append(throughput)
                r_latency_series.append(avg_latency)
            read_throughput_results.append({"label": f"R={r}", "data": r_throughput_series})
            read_latency_results.append({"label": f"R={r}", "data": r_latency_series})

        # Plotting Helper
        def plot_results(results, title, ylabel, filename):
            plt.figure(figsize=(10, 6))
            # Use a colormap for distinct lines
            colors = plt.cm.viridis(np.linspace(0, 0.9, len(results)))
            
            for idx, res in enumerate(results):
                plt.plot([f"{fr*100:.0f}%" for fr in failure_rates], res["data"], 
                        marker='o', linewidth=2, label=res["label"], color=colors[idx])

            plt.title(title)
            plt.xlabel('Node Failure Rate (%)')
            plt.ylabel(ylabel)
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.legend()
            plt.tight_layout()
            plt.savefig(filename)
            print(f"✅ Saved {filename}")

        # Plot Throughput
        plot_results(write_throughput_results, f'Impact of Failures on Write Throughput (N={n})', 'Throughput (Ops/Sec)', f"graph_failure_impact_write_throughput_Cluster{CLUSTER_SIZE}_N{n}.png")
        plot_results(read_throughput_results, f'Impact of Failures on Read Throughput (N={n})', 'Throughput (Ops/Sec)', f"graph_failure_impact_read_throughput_Cluster{CLUSTER_SIZE}_N{n}.png")
        
        # Plot Latency
        plot_results(write_latency_results, f'Impact of Failures on Write Latency (N={n})', 'Latency (ms)', f"graph_failure_impact_write_latency_Cluster{CLUSTER_SIZE}_N{n}.png")
        plot_results(read_latency_results, f'Impact of Failures on Read Latency (N={n})', 'Latency (ms)', f"graph_failure_impact_read_latency_Cluster{CLUSTER_SIZE}_N{n}.png")

if __name__ == "__main__":
    # run_million_keys_simulation()
    # run_read_simulation()
    run_failure_impact_simulation()

if __name__ == "__main__":
    # run_theoretical_benchmark() # Commented out
    # run_million_keys_simulation()
    # run_read_simulation()
    run_failure_impact_simulation()
