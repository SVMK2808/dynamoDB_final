#!/usr/bin/env python3
"""
simulate_scale.py - Large Scale Cluster Simulation Tool

This script simulates high-volume traffic against a DynamoDB-style cluster.
It is designed to test:
1. "Thousands of nodes" (by targeting ports 7000 to 7000+N)
2. "Millions of objects" (by generating a massive key space)

Usage:
    python3 simulate_scale.py --nodes 10 --keys 10000 --workers 20

Note: Running 1000 actual nodes on a single machine is usually impossible due to OS limits.
This script assumes the cluster is already running.
"""

import argparse
import random
import string
import time
import requests
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Configuration
BASE_PORT = 8000

def generate_key(index):
    """Generate a deterministic but distributed key based on index"""
    # Use a deterministic suffix (like a hash of the index) to ensure distribution
    # without using random numbers that change between write/read phases
    suffix = (index * 2654435761) % 10000 # Knuth's multiplicative hash
    return f"key-{index}-{suffix}"

def generate_value(size_bytes=100):
    """Generate a random string value"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size_bytes))

def get_random_node_url(num_nodes):
    """Pick a random node from the cluster to send the request to"""
    # In a real client, we might be smarter, but a load balancer 
    # or random selection is a standard strategy
    port = BASE_PORT + random.randint(0, num_nodes - 1)
    return f"http://localhost:{port}"

def perform_put(node_url, key, value):
    try:
        resp = requests.put(
            f"{node_url}/kv/{key}",
            json={"value": value},
            headers={"Content-Type": "application/json"},
            timeout=2  # Short timeout for high throughput
        )
        return resp.status_code in (200, 201)
    except Exception:
        return False

def perform_get(node_url, key):
    try:
        resp = requests.get(
            f"{node_url}/kv/{key}",
            timeout=2
        )
        return resp.status_code == 200
    except Exception:
        return False

def run_simulation(num_nodes, num_keys, num_workers, value_size):
    print("🚀 Starting Scale Simulation")
    print(f"   Target Cluster: {num_nodes} nodes (Ports {BASE_PORT}-{BASE_PORT+num_nodes-1})")
    print(f"   Key Space:      {num_keys:,} keys")
    print(f"   Concurrency:    {num_workers} workers")
    print("-" * 60)

    # 1. WRITE PHASE
    print(f"\n📝 Phase 1: Writing {num_keys:,} keys...")
    start_time = time.time()
    success_writes = 0
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        # Submit all tasks
        for i in range(num_keys):
            key = generate_key(i)
            val = generate_value(value_size)
            url = get_random_node_url(num_nodes)
            futures.append(executor.submit(perform_put, url, key, val))
        
        # Process results as they complete
        for f in tqdm(as_completed(futures), total=num_keys, unit="ops"):
            if f.result():
                success_writes += 1
    
    write_duration = time.time() - start_time
    print(f"   ✅ Writes Completed: {success_writes}/{num_keys} ({success_writes/num_keys*100:.1f}%)")
    print(f"   ⏱️  Duration: {write_duration:.2f}s ({success_writes/write_duration:.1f} ops/sec)")

    # 2. READ PHASE
    print(f"\n📖 Phase 2: Reading {num_keys:,} keys...")
    start_time = time.time()
    success_reads = 0
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i in range(num_keys):
            key = generate_key(i)
            # We don't need the value for GET, just the key
            url = get_random_node_url(num_nodes)
            futures.append(executor.submit(perform_get, url, key))
        
        for f in tqdm(as_completed(futures), total=num_keys, unit="ops"):
            if f.result():
                success_reads += 1

    read_duration = time.time() - start_time
    print(f"   ✅ Reads Completed: {success_reads}/{num_keys} ({success_reads/num_keys*100:.1f}%)")
    print(f"   ⏱️  Duration: {read_duration:.2f}s ({success_reads/read_duration:.1f} ops/sec)")

    return {
        "write_ops_sec": success_writes / write_duration if write_duration > 0 else 0,
        "read_ops_sec": success_reads / read_duration if read_duration > 0 else 0,
        "write_success_rate": success_writes / num_keys,
        "read_success_rate": success_reads / num_keys
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DynamoDB Scale Simulator")
    parser.add_argument("--nodes", type=int, default=4, help="Number of nodes in the running cluster")
    parser.add_argument("--keys", type=int, default=1000, help="Number of keys to simulate")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent threads")
    parser.add_argument("--size", type=int, default=100, help="Size of value in bytes")
    
    args = parser.parse_args()
    
    run_simulation(args.nodes, args.keys, args.workers, args.size)
