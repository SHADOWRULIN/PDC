"""
Parallel & Distributed Computing - Assignment (Chapter 3)
Topic: Process-Based Parallelism & IPC using Bounded JoinableQueue
Reference: Chapter 3 (Using a queue to exchange data), Python Parallel Programming Cookbook (2nd Ed.)

Enhancements over Original Cookbook Recipe (`communicating_with_queue.py`):
1. Bounded JoinableQueue: Added `maxsize=5` to prevent unbounded memory growth and used
   `task_done()` / `join()` to track item processing completion across processes.
2. Multi-Process Subclassing: Scaled from 1 producer/1 consumer to 3 ProducerProcess and
   2 ConsumerProcess workers inheriting from `multiprocessing.Process`, logging OS PIDs.
3. Poison-Pill Shutdown Protocol: Replaced the fragile `queue.empty()` check (which can
   cause consumers to exit prematurely) with explicit `None` sentinel values.
4. Synchronized Trace Logging: Used `multiprocessing.Lock()` with timestamps to prevent
   interleaved terminal output across independent OS processes.
"""

import multiprocessing
import os
import random
import time
from datetime import datetime

# Shared Configuration
QUEUE_CAPACITY = 5
NUM_PRODUCERS = 3
NUM_CONSUMERS = 2
ITEMS_PER_PRODUCER = 5  # Total items = 15


def log_event(lock, proc_name: str, action: str, details: str):
    """Thread- and process-safe timestamped logger displaying OS Process ID."""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    pid = os.getpid()
    with lock:
        print(f"[{timestamp}] [PID:{pid:<6}] [{proc_name:^12}] {action:<15} | {details}")


class ProducerProcess(multiprocessing.Process):
    """Subclasses multiprocessing.Process to generate items into a bounded queue."""

    def __init__(self, producer_id: int, task_queue: multiprocessing.JoinableQueue,
                 num_items: int, print_lock):
        super().__init__(name=f"Producer-{producer_id}")
        self.producer_id = producer_id
        self.task_queue = task_queue
        self.num_items = num_items
        self.print_lock = print_lock

    def run(self):
        log_event(self.print_lock, self.name, "STARTED", "Process initialized.")
        for i in range(1, self.num_items + 1):
            # Simulate data generation workload
            time.sleep(random.uniform(0.1, 0.3))
            item = f"P{self.producer_id}-Data#{i}({random.randint(10, 99)})"

            # Blocks automatically if the queue has reached QUEUE_CAPACITY
            self.task_queue.put(item)
            log_event(
                self.print_lock,
                self.name,
                "ENQUEUED",
                f"Added {item} | Approx Queue Size: {self.task_queue.qsize()}/{QUEUE_CAPACITY}",
            )

        log_event(self.print_lock, self.name, "FINISHED", "Completed producing assigned items.")


class ConsumerProcess(multiprocessing.Process):
    """Subclasses multiprocessing.Process to retrieve and process items until sentinel."""

    def __init__(self, consumer_id: int, task_queue: multiprocessing.JoinableQueue, print_lock):
        super().__init__(name=f"Consumer-{consumer_id}")
        self.task_queue = task_queue
        self.print_lock = print_lock

    def run(self):
        log_event(self.print_lock, self.name, "STARTED", "Waiting for items in queue.")
        while True:
            # Blocks safely until an item (or Poison-Pill sentinel) is available
            item = self.task_queue.get()

            # Check for Poison-Pill sentinel signaling graceful shutdown
            if item is None:
                self.task_queue.task_done()
                log_event(self.print_lock, self.name, "SHUTDOWN", "Received sentinel. Exiting.")
                break

            log_event(
                self.print_lock,
                self.name,
                "DEQUEUED",
                f"Processing {item} | Remaining: {self.task_queue.qsize()}",
            )
            # Simulate item processing workload
            time.sleep(random.uniform(0.15, 0.35))
            self.task_queue.task_done()


def main():
    print("=" * 84)
    print("  PARALLEL & DISTRIBUTED COMPUTING (CHAPTER 3): MULTIPROCESSING BOUNDED QUEUE")
    print(f"  Config: Queue MaxSize={QUEUE_CAPACITY} | Producers={NUM_PRODUCERS} | Consumers={NUM_CONSUMERS}")
    print("=" * 84)

    start_time = time.time()

    # Initialize IPC Queue and Process Synchronization Lock
    task_queue = multiprocessing.JoinableQueue(maxsize=QUEUE_CAPACITY)
    print_lock = multiprocessing.Lock()

    # Instantiate Producer and Consumer Processes
    producers = [
        ProducerProcess(i + 1, task_queue, ITEMS_PER_PRODUCER, print_lock)
        for i in range(NUM_PRODUCERS)
    ]
    consumers = [
        ConsumerProcess(i + 1, task_queue, print_lock)
        for i in range(NUM_CONSUMERS)
    ]

    # Spawn all child processes
    for c in consumers:
        c.start()
    for p in producers:
        p.start()

    # Wait for all producers to finish generating data
    for p in producers:
        p.join()

    # Send one Poison-Pill (None) per consumer to terminate them gracefully
    for _ in range(NUM_CONSUMERS):
        task_queue.put(None)

    # Block until all items and sentinels have been marked task_done()
    task_queue.join()

    # Ensure all consumer processes have exited cleanly
    for c in consumers:
        c.join()

    elapsed = time.time() - start_time
    print("=" * 84)
    print(f"All processes completed cleanly in {elapsed:.2f} seconds.")
    print("=" * 84)


if __name__ == "__main__":
    main()