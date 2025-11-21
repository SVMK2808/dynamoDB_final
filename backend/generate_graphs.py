#!/usr/bin/env python3
import os
import time
import subprocess
import matplotlib.pyplot as plt
import simulate_scale

# Configuration
NUM_NODES = 4
KEYS = 500
WORKERS = 10
CLUSTER_SCRIPT = "./run_cluster.sh"

def run_benchmark(n, r, w):
    print(f"\n\n📊 Benchmarking Configuration: N={n}, R={r}, W={w}")
    
    # 1. Stop existing cluster
    subprocess.run([CLUSTER_SCRIPT, "stop"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    
    # 2. Start cluster with new config
    env = os.environ.copy()
    env["NUM_NODES"] = str(NUM_NODES)
    env["N"] = str(n)
    env["R"] = str(r)
    env["W"] = str(w)
    
    print("   Starting cluster...")
    subprocess.run([CLUSTER_SCRIPT], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # Wait for cluster to stabilize
    print("   Waiting 15s for cluster stabilization...")
    time.sleep(15)
    
    # 3. Run simulation
    print("   Running workload...")
    results = simulate_scale.run_simulation(NUM_NODES, KEYS, WORKERS, 100)
    
    return results

def plot_results(scenarios, write_ops, read_ops, title, filename):
    labels = [f"N={s[0]} R={s[1]} W={s[2]}" for s in scenarios]
    
    x = range(len(labels))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(10, 6))
    rects1 = ax.bar([i - width/2 for i in x], write_ops, width, label='Write Ops/Sec', color='#ff9999')
    rects2 = ax.bar([i + width/2 for i in x], read_ops, width, label='Read Ops/Sec', color='#66b3ff')
    
    ax.set_ylabel('Throughput (Operations/Second)')
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    
    # Add value labels
    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate(f'{int(height)}',
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)
    
    fig.tight_layout()
    plt.savefig(filename)
    print(f"\n✅ Graph saved to {filename}")

def main():
    # Scenario 1: Consistency Levels (Fixed N=3)
    scenarios_consistency = [
        (3, 1, 1), # Weak
        (3, 2, 2), # Quorum
        (3, 3, 3)  # Strong
    ]
    
    writes = []
    reads = []
    
    for n, r, w in scenarios_consistency:
        res = run_benchmark(n, r, w)
        writes.append(res["write_ops_sec"])
        reads.append(res["read_ops_sec"])
        
    plot_results(scenarios_consistency, writes, reads, 
                 "Performance vs Consistency (N=3)", "graph_consistency.png")

    # Scenario 2: Replication Cost (Fixed R=1, W=1)
    scenarios_replication = [
        (1, 1, 1),
        (3, 1, 1),
        (4, 1, 1)
    ]
    
    writes_rep = []
    reads_rep = []
    
    for n, r, w in scenarios_replication:
        res = run_benchmark(n, r, w)
        writes_rep.append(res["write_ops_sec"])
        reads_rep.append(res["read_ops_sec"])
        
    plot_results(scenarios_replication, writes_rep, reads_rep, 
                 "Performance vs Replication Factor (R=1, W=1)", "graph_replication.png")

if __name__ == "__main__":
    main()
