"""
Parallel & Distributed Computing - Assignment 2
Topic: Thread Synchronization using Counting Semaphores and Mutex Locks
Reference: Chapter 2 (Thread-based Parallelism), Python Parallel Programming Cookbook

Enhancements Implemented:
1. Replaced arbitrary sleep synchronization with dual counting semaphores (empty/full slots).
2. Added a mutex lock (threading.Lock) to guarantee atomic critical-section buffer access.
3. Implemented a coordinated exit protocol using threading.Event for clean thread termination.
4. Added structured, timestamped trace logging to monitor buffer state transitions in real time.
"""

import threading
import time
import random
from datetime import datetime

# Shared Configuration
BUFFER_CAPACITY = 5
TOTAL_ITEMS_TO_PRODUCE = 15

# Shared Resources & Synchronization Primitives
buffer = []
buffer_lock = threading.Lock()                     # Mutex: Protects atomic access to shared buffer
empty_slots = threading.Semaphore(BUFFER_CAPACITY) # Counts available empty buffer slots
full_slots = threading.Semaphore(0)                # Counts populated items ready for consumption
shutdown_event = threading.Event()                 # Signals worker threads to terminate gracefully


def log_event(thread_name: str, action: str, details: str):
    """Outputs timestamped thread trace information."""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] [{thread_name:^12}] {action:<18} | {details}")


def producer(producer_id: int, items_per_producer: int):
    """
    Produces data items and places them into the bounded buffer.
    Blocks if the buffer has reached maximum capacity.
    """
    thread_name = f"Producer-{producer_id}"
    log_event(thread_name, "STARTED", "Worker thread initialized.")

    for i in range(items_per_producer):
        item = f"P{producer_id}-Task#{i + 1}"
        
        # Simulate variable workload before insertion
        time.sleep(random.uniform(0.1, 0.4))

        # 1. Wait for an empty slot in the buffer
        empty_slots.acquire()

        # 2. Critical Section: Insert item into shared buffer
        with buffer_lock:
            buffer.append(item)
            current_size = len(buffer)
            log_event(thread_name, "ITEM INSERTED", f"Item: {item} | Buffer State: {current_size}/{BUFFER_CAPACITY}")

        # 3. Signal consumers that a new item is ready
        full_slots.release()

    log_event(thread_name, "FINISHED", "All designated items produced.")


def consumer(consumer_id: int):
    """
    Retrieves and processes items from the shared buffer.
    Blocks if the buffer is empty; terminates upon receiving the shutdown signal.
    """
    thread_name = f"Consumer-{consumer_id}"
    log_event(thread_name, "STARTED", "Worker thread initialized.")

    while not shutdown_event.is_set() or len(buffer) > 0:
        # Attempt to acquire an available item with a timeout to prevent deadlocks on exit
        acquired = full_slots.acquire(timeout=0.5)
        if not acquired:
            continue

        # Critical Section: Extract item from shared buffer
        with buffer_lock:
            if buffer:
                item = buffer.pop(0)
                current_size = len(buffer)
                log_event(thread_name, "ITEM CONSUMED", f"Item: {item} | Buffer State: {current_size}/{BUFFER_CAPACITY}")
            else:
                item = None

        if item:
            # 1. Signal producers that a slot has freed up
            empty_slots.release()

            # 2. Simulate processing overhead
            time.sleep(random.uniform(0.2, 0.5))

    log_event(thread_name, "FINISHED", "Consumer terminated cleanly.")


def main():
    print("=" * 75)
    print("  PARALLEL & DISTRIBUTED COMPUTING: BOUNDED-BUFFER SEMAPHORE SYNCHRONIZATION")
    print(f"  Configuration: Buffer Size = {BUFFER_CAPACITY}, Total Workload = {TOTAL_ITEMS_TO_PRODUCE} items")
    print("=" * 75)

    start_time = time.time()

    # Worker allocation
    num_producers = 3
    num_consumers = 2
    items_per_producer = TOTAL_ITEMS_TO_PRODUCE // num_producers

    producers = [
        threading.Thread(target=producer, args=(i + 1, items_per_producer), name=f"Producer-{i + 1}")
        for i in range(num_producers)
    ]
    consumers = [
        threading.Thread(target=consumer, args=(i + 1,), name=f"Consumer-{i + 1}")
        for i in range(num_consumers)
    ]

    # Start all concurrent workers
    for c in consumers:
        c.start()
    for p in producers:
        p.start()

    # Wait for all producers to finish inserting data
    for p in producers:
        p.join()

    # Signal consumers to finish pending buffer items and shut down
    shutdown_event.set()

    # Wait for all consumers to deplete the buffer and terminate
    for c in consumers:
        c.join()

    elapsed = time.time() - start_time
    print("=" * 75)
    print(f"Execution completed in {elapsed:.2f} seconds.")
    print(f"Final Buffer State: {len(buffer)} items remaining.")
    print("=" * 75)


if __name__ == "__main__":
    main()