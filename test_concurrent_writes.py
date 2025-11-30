import requests
import json
import time
import threading

# Configuration
NODES = [
    "http://localhost:8000",
    "http://localhost:8001",
    "http://localhost:8002"
]
KEY = "test_key_concurrent"

def put_value(node_url, key, value):
    url = f"{node_url}/kv/{key}"
    payload = {"value": value}
    headers = {"Content-Type": "application/json"}
    
    print(f"Writing '{value}' to {node_url}...")
    try:
        response = requests.put(url, json=payload, headers=headers, timeout=2)
        print(f"Node {node_url} responded: {response.status_code}")
    except Exception as e:
        print(f"Error writing to {node_url}: {e}")

def get_value(node_url, key):
    url = f"{node_url}/kv/{key}"
    print(f"Reading from {node_url}...")
    try:
        response = requests.get(url, timeout=2)
        if response.status_code == 200:
            data = response.json()
            print(f"Result from {node_url}:")
            print(f"  Value: {data.get('value')}")
            print(f"  Vector Clock: {data.get('vector_clock')}")
            if "conflicts" in data:
                print(f"  Conflicts: {data['conflicts']}")
        else:
            print(f"Failed to read from {node_url}: {response.status_code}")
    except Exception as e:
        print(f"Error reading from {node_url}: {e}")

def main():
    # 1. Concurrent Writes to different nodes
    # This simulates a network partition or high concurrency where updates hit different coordinators
    t1 = threading.Thread(target=put_value, args=(NODES[0], KEY, "concurrent_val_A"))
    t2 = threading.Thread(target=put_value, args=(NODES[1], KEY, "concurrent_val_B"))
    
    print("Starting concurrent writes...")
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()
    
    print("Writes completed. Waiting for propagation...")
    time.sleep(2)
    
    # 2. Read back to see if we have conflicts or a resolved value
    print("-" * 30)
    get_value(NODES[0], KEY)

if __name__ == "__main__":
    main()
