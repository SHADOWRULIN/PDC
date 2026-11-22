# Parallel & Distributed Computing — Assignment (Chapter 3)
**Topic:** Process-Based Parallelism & Inter-Process Communication (IPC)  
**Implementation:** Multi-Process Producer-Consumer using Bounded `JoinableQueue` and Process Subclassing

---

## 1. Overview
This project is an academic submission for the **Parallel & Distributed Computing** course, adapted from the **Using a queue to exchange data** recipe (`communicating_with_queue.py`) in **Chapter 3** of the *Python Parallel Programming Cookbook (Second Edition)* by Packt Publishing[cite: 314, 317].

Because separate OS processes maintain completely isolated memory spaces, they cannot share variables directly like threads[cite: 258, 451]. This implementation demonstrates safe, multi-process data exchange using `multiprocessing.JoinableQueue`[cite: 317].

---

## 2. Key Modifications & Enhancements
1. **Bounded `JoinableQueue(maxsize=5)`:**
   - Enforces a strict capacity limit so fast producers automatically block when the queue is full, preventing unbounded memory consumption[cite: 458].
   - Uses `task_done()` and `join()` to ensure all queued tasks are fully processed before program termination[cite: 317, 458].
2. **Object-Oriented Process Subclassing:**
   - Scaled from a single producer/consumer pair to **3 `ProducerProcess`** and **2 `ConsumerProcess`** workers inheriting from `multiprocessing.Process` and overriding `run()`[cite: 312, 458].
   - Logs unique OS Process IDs (`os.getpid()`) to verify true multi-process execution across CPU cores[cite: 458].
3. **Deterministic Poison-Pill Shutdown:**
   - Replaced the original recipe's `queue.empty()` check (which can cause consumers to exit early if producers are slower) with explicit `None` sentinel values—one per consumer[cite: 316, 458].
4. **Synchronized Terminal Logging:**
   - Added a `multiprocessing.Lock()` around `print()` statements with millisecond timestamps to prevent scrambled output across concurrent processes[cite: 321, 458].

---

## 3. IPC Architecture

```text
[ProducerProcess 1, 2, 3]  (Separate OS PIDs)
           │
           ▼  put(item) [Blocks if size == 5]
┌────────────────────────────────────────────────────────┐
│      multiprocessing.JoinableQueue (maxsize=5)         │
│         [ Item 1 | Item 2 | Item 3 | ... ]             │
└────────────────────────────────────────────────────────┘
           │
           ▼  get() + task_done() [Exits on None sentinel]
[ConsumerProcess 1, 2]     (Separate OS PIDs)

```

---

## 4. How to Run

Ensure Python 3.8+ is installed, then run the script from your terminal:

```bash
cd "PDC/Chapter 3"
python3 process_queue_producer_consumer.py

```

### Sample Output Trace

```text
====================================================================================
  PARALLEL & DISTRIBUTED COMPUTING (CHAPTER 3): MULTIPROCESSING BOUNDED QUEUE
  Config: Queue MaxSize=5 | Producers=3 | Consumers=2
====================================================================================
[18:59:51.684] [PID:29    ] [ Consumer-1 ] STARTED         | Waiting for items in queue.
[18:59:51.691] [PID:31    ] [ Producer-1 ] STARTED         | Process initialized.
[18:59:51.823] [PID:31    ] [ Producer-1 ] ENQUEUED        | Added P1-Data#1(75) | Approx Queue Size: 1/5
[18:59:51.825] [PID:29    ] [ Consumer-1 ] DEQUEUED        | Processing P1-Data#1(75) | Remaining: 0
...
[18:59:53.689] [PID:29    ] [ Consumer-1 ] SHUTDOWN        | Received sentinel. Exiting.
[18:59:53.781] [PID:30    ] [ Consumer-2 ] SHUTDOWN        | Received sentinel. Exiting.
====================================================================================
All processes completed cleanly in 2.14 seconds.
====================================================================================

```
