
import numpy as np
import matplotlib.pyplot as plt
import time

# Simulation Constants
CLUSTER_SIZE = 1000
NUM_KEYS = 1_000_000
BATCH_SIZE = 1000

# Latency Model (in milliseconds)
NETWORK_MEAN = 20.0
NETWORK_STD = 5.0
WRITE_DISK_MEAN = 2.0
WRITE_DISK_STD = 0.5
READ_DISK_MEAN = 8.0
READ_DISK_STD = 2.0
READ_RECONCILIATION_BASE = 0.05 
FANOUT_COST_PER_NODE = 0.01
TIMEOUT_MS = 1000.0
CLIENT_RETRY_DELAY = 50.0

def simulate_latency(n, quorum_size, op_type="write", num_ops=1000, node_failure_rate=0.0, coord_failure_rate=0.0, network_mean=20.0):
    # 1. Network RTT
    net_delays = np.random.normal(network_mean, NETWORK_STD, (num_ops, n))
    net_delays = np.maximum(net_delays, 1.0)
    
    # 2. Disk Processing
    if op_type == "write":
        disk_mean, disk_std = WRITE_DISK_MEAN, WRITE_DISK_STD
    else:
        disk_mean, disk_std = READ_DISK_MEAN, READ_DISK_STD

    disk_delays = np.random.normal(disk_mean, disk_std, (num_ops, n))
    disk_delays = np.maximum(disk_delays, 0.5)
    
    total_delays = net_delays + disk_delays

    # 3. Simulate Node Failures
    if node_failure_rate > 0.0:
        is_failed = np.random.random((num_ops, n)) < node_failure_rate
        total_delays[is_failed] = TIMEOUT_MS
    
    total_delays.sort(axis=1)
    op_latencies = total_delays[:, quorum_size - 1]
    
    fanout_overhead = n * FANOUT_COST_PER_NODE
    op_latencies += fanout_overhead

    if op_type == "read" and quorum_size > 1:
        overhead = (quorum_size - 1) * READ_RECONCILIATION_BASE
        op_latencies += overhead

    if coord_failure_rate > 0.0:
        coord_failed = np.random.random(num_ops) < coord_failure_rate
        op_latencies[coord_failed] += CLIENT_RETRY_DELAY

    if op_type == "read" and node_failure_rate > 0.0:
        needs_repair = np.random.random(num_ops) < node_failure_rate
        repair_cost = network_mean + WRITE_DISK_MEAN
        op_latencies[needs_repair] += repair_cost

    return np.mean(op_latencies)

def run_part1_cluster_size_impact():
    print("\n🚀 Starting Part 1: Cluster Size Impact Simulation")
    
    cluster_sizes = [100, 500, 1000]
    failure_rates = [0.0, 0.05, 0.10, 0.20]
    n = 3
    w = 2 # Majority
    r = 2 # Majority
    
    # Data storage
    write_throughput_data = []
    write_latency_data = []
    read_throughput_data = []
    read_latency_data = []

    for cs in cluster_sizes:
        # Assume network latency increases slightly with cluster size
        # Base 20ms + 1ms per 100 nodes
        current_network_mean = 20.0 + (cs / 100.0)
        
        w_throughputs = []
        w_latencies = []
        r_throughputs = []
        r_latencies = []
        
        for fr in failure_rates:
            # Write
            lat_w = simulate_latency(n, w, "write", 2000, fr, 0.01, current_network_mean)
            thr_w = (1000 / lat_w) * 100
            w_throughputs.append(thr_w)
            w_latencies.append(lat_w)
            
            # Read
            lat_r = simulate_latency(n, r, "read", 2000, fr, 0.01, current_network_mean)
            thr_r = (1000 / lat_r) * 100
            r_throughputs.append(thr_r)
            r_latencies.append(lat_r)
            
        write_throughput_data.append({"label": f"Cluster Size={cs}", "data": w_throughputs})
        write_latency_data.append({"label": f"Cluster Size={cs}", "data": w_latencies})
        read_throughput_data.append({"label": f"Cluster Size={cs}", "data": r_throughputs})
        read_latency_data.append({"label": f"Cluster Size={cs}", "data": r_latencies})

    # Plotting
    def plot_graph(data, title, ylabel, filename):
        plt.figure(figsize=(10, 6))
        colors = plt.cm.viridis(np.linspace(0, 0.9, len(data)))
        for idx, item in enumerate(data):
            plt.plot([f"{fr*100:.0f}%" for fr in failure_rates], item["data"], 
                    marker='o', linewidth=2, label=item["label"], color=colors[idx])
        plt.title(title)
        plt.xlabel('Node Failure Rate (%)')
        plt.ylabel(ylabel)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend()
        plt.tight_layout()
        plt.savefig(filename)
        print(f"✅ Saved {filename}")

    plot_graph(write_throughput_data, 'Write Throughput vs Failure Rate (Different Cluster Sizes)', 'Throughput (Ops/Sec)', 'part1_write_throughput.png')
    plot_graph(write_latency_data, 'Write Latency vs Failure Rate (Different Cluster Sizes)', 'Latency (ms)', 'part1_write_latency.png')
    plot_graph(read_throughput_data, 'Read Throughput vs Failure Rate (Different Cluster Sizes)', 'Throughput (Ops/Sec)', 'part1_read_throughput.png')
    plot_graph(read_latency_data, 'Read Latency vs Failure Rate (Different Cluster Sizes)', 'Latency (ms)', 'part1_read_latency.png')

def run_part2_n_impact():
    print("\n🚀 Starting Part 2: N Impact Simulation")
    
    n_values = [10, 20, 50]
    failure_rates = [0.0, 0.05, 0.10, 0.20]
    cluster_size = 1000
    network_mean = 20.0 + (cluster_size / 100.0) # Fixed for this part
    
    # Data storage
    write_throughput_data = []
    write_latency_data = []
    read_throughput_data = []
    read_latency_data = []

    for n in n_values:
        w = (n // 2) + 1 # Majority
        r = (n // 2) + 1 # Majority
        
        w_throughputs = []
        w_latencies = []
        r_throughputs = []
        r_latencies = []
        
        for fr in failure_rates:
            # Write
            lat_w = simulate_latency(n, w, "write", 2000, fr, 0.01, network_mean)
            thr_w = (1000 / lat_w) * 100
            w_throughputs.append(thr_w)
            w_latencies.append(lat_w)
            
            # Read
            lat_r = simulate_latency(n, r, "read", 2000, fr, 0.01, network_mean)
            thr_r = (1000 / lat_r) * 100
            r_throughputs.append(thr_r)
            r_latencies.append(lat_r)
            
        write_throughput_data.append({"label": f"N={n}", "data": w_throughputs})
        write_latency_data.append({"label": f"N={n}", "data": w_latencies})
        read_throughput_data.append({"label": f"N={n}", "data": r_throughputs})
        read_latency_data.append({"label": f"N={n}", "data": r_latencies})

    # Plotting
    def plot_graph(data, title, ylabel, filename):
        plt.figure(figsize=(10, 6))
        colors = plt.cm.plasma(np.linspace(0, 0.9, len(data)))
        for idx, item in enumerate(data):
            plt.plot([f"{fr*100:.0f}%" for fr in failure_rates], item["data"], 
                    marker='s', linewidth=2, label=item["label"], color=colors[idx])
        plt.title(title)
        plt.xlabel('Node Failure Rate (%)')
        plt.ylabel(ylabel)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend()
        plt.tight_layout()
        plt.savefig(filename)
        print(f"✅ Saved {filename}")

    plot_graph(write_throughput_data, 'Write Throughput vs Failure Rate (Different N)', 'Throughput (Ops/Sec)', 'part2_write_throughput.png')
    plot_graph(write_latency_data, 'Write Latency vs Failure Rate (Different N)', 'Latency (ms)', 'part2_write_latency.png')
    plot_graph(read_throughput_data, 'Read Throughput vs Failure Rate (Different N)', 'Throughput (Ops/Sec)', 'part2_read_throughput.png')
    plot_graph(read_latency_data, 'Read Latency vs Failure Rate (Different N)', 'Latency (ms)', 'part2_read_latency.png')

if __name__ == "__main__":
    run_part1_cluster_size_impact()
    run_part2_n_impact()
