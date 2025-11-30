import requests
import json
import time
import random
import string

# Configuration
BASE_URL = "http://localhost:8000"
KEY = "test_key_sequential"

def generate_random_value(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def put_value(key, value):
    url = f"{BASE_URL}/kv/{key}"
    payload = {"value": value}
    headers = {"Content-Type": "application/json"}
    
    print(f"Writing value '{value}' to key '{key}'...")
    try:
        response = requests.put(url, json=payload, headers=headers)
        if response.status_code in [200, 201]:
            print(f"Success: {response.json()}")
        else:
            print(f"Failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error: {e}")

def get_value(key):
    url = f"{BASE_URL}/kv/{key}"
    print(f"Reading key '{key}'...")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            print(f"Current Value: {data.get('value')}")
            print(f"Vector Clock: {data.get('vector_clock')}")
            if "conflicts" in data:
                print(f"Conflicts: {data['conflicts']}")
        else:
            print(f"Failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error: {e}")

def main():
    # 1. Initial Write
    val1 = "value_1_" + generate_random_value(5)
    put_value(KEY, val1)
    time.sleep(1) # Allow for propagation
    get_value(KEY)
    print("-" * 30)

    # 2. Second Write (Update)
    val2 = "value_2_" + generate_random_value(5)
    put_value(KEY, val2)
    time.sleep(1)
    get_value(KEY)
    print("-" * 30)

    # 3. Third Write (Update)
    val3 = "value_3_" + generate_random_value(5)
    put_value(KEY, val3)
    time.sleep(1)
    get_value(KEY)
    print("-" * 30)

if __name__ == "__main__":
    main()
