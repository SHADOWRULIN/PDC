# Parallel & Distributed Computing — Assignment 2
**Topic:** Thread-based Parallelism & Synchronization (Chapter 2)  
**Implementation:** Enhanced Bounded-Buffer Producer-Consumer using Counting Semaphores and Mutex Locks

---

## 1. Overview
This project is an academic submission for the Parallel & Distributed Computing course. It is based on the thread synchronization paradigms presented in **Chapter 2** of the *Python Parallel Programming Cookbook (Second Edition)* by Packt Publishing.

The original recipe demonstrates elementary usage of `threading.Semaphore` with unbounded shared lists. This project extends that concept into a production-grade, bounded-buffer producer-consumer architecture that avoids buffer underflows, buffer overflows, and race conditions.

---

## 2. Key Modifications & Enhancements
1. **Dual Counting Semaphores:**
   - `empty_slots` initialized to `BUFFER_CAPACITY (5)` to enforce a strict upper bound on memory usage.
   - `full_slots` initialized to `0` to block consumers immediately whenever the buffer is starved of data.
2. **Critical Section Protection:**
   - Implemented `threading.Lock()` across all buffer mutations (`append` and `pop(0)`) to guarantee strict atomicity and eliminate race conditions on shared pointers.
3. **Graceful Shutdown Protocol:**
   - Used `threading.Event()` combined with a timed acquire pattern on semaphores to eliminate thread hangs and deadlocks upon task completion.
4. **Diagnostic Logging:**
   - Added microsecond-accurate timestamped tracing showing thread IDs, operations performed, and live buffer capacity states.

---

## 3. Synchronization Architecture


```

[Producers: P1, P2, P3]
│
▼ (Acquire empty_slots)
┌────────────────────────────────────────────────────────┐
│             MUTEX LOCK (buffer_lock)                   │
│   Shared Circular Buffer [Capacity: 5 Elements]        │
└────────────────────────────────────────────────────────┘
▲ (Release empty_slots)
│
[Consumers: C1, C2] ◄─── (Acquire full_slots)

```

---

## 4. How to Run
Ensure Python 3.8+ is installed on your machine:

```bash
cd "Chapter 2"
python3 thread_synchronization_semaphore.py

```

### Output Trace

```text
===========================================================================
  PARALLEL & DISTRIBUTED COMPUTING: BOUNDED-BUFFER SEMAPHORE SYNCHRONIZATION
  Configuration: Buffer Size = 5, Total Workload = 15 items
===========================================================================
[19:51:12.750] [ Consumer-1 ] STARTED            | Worker thread initialized.
[19:51:12.751] [ Consumer-2 ] STARTED            | Worker thread initialized.
[19:51:12.754] [ Producer-1 ] STARTED            | Worker thread initialized.
[19:51:12.754] [ Producer-2 ] STARTED            | Worker thread initialized.
[19:51:12.755] [ Producer-3 ] STARTED            | Worker thread initialized.
[19:51:12.858] [ Producer-2 ] ITEM INSERTED      | Item: P2-Task#1 | Buffer State: 1/5
[19:51:12.858] [ Consumer-1 ] ITEM CONSUMED      | Item: P2-Task#1 | Buffer State: 0/5
[19:51:13.016] [ Producer-2 ] ITEM INSERTED      | Item: P2-Task#2 | Buffer State: 1/5
[19:51:13.016] [ Consumer-2 ] ITEM CONSUMED      | Item: P2-Task#2 | Buffer State: 0/5
[19:51:13.022] [ Producer-3 ] ITEM INSERTED      | Item: P3-Task#1 | Buffer State: 1/5
[19:51:13.037] [ Producer-1 ] ITEM INSERTED      | Item: P1-Task#1 | Buffer State: 2/5
[19:51:13.064] [ Consumer-1 ] ITEM CONSUMED      | Item: P3-Task#1 | Buffer State: 1/5
[19:51:13.273] [ Consumer-2 ] ITEM CONSUMED      | Item: P1-Task#1 | Buffer State: 0/5
[19:51:13.317] [ Producer-2 ] ITEM INSERTED      | Item: P2-Task#3 | Buffer State: 1/5
[19:51:13.363] [ Producer-1 ] ITEM INSERTED      | Item: P1-Task#2 | Buffer State: 2/5
[19:51:13.404] [ Producer-3 ] ITEM INSERTED      | Item: P3-Task#2 | Buffer State: 3/5
[19:51:13.479] [ Consumer-1 ] ITEM CONSUMED      | Item: P2-Task#3 | Buffer State: 2/5
[19:51:13.490] [ Producer-2 ] ITEM INSERTED      | Item: P2-Task#4 | Buffer State: 3/5
[19:51:13.525] [ Producer-3 ] ITEM INSERTED      | Item: P3-Task#3 | Buffer State: 4/5
[19:51:13.531] [ Producer-1 ] ITEM INSERTED      | Item: P1-Task#3 | Buffer State: 5/5
[19:51:13.579] [ Consumer-2 ] ITEM CONSUMED      | Item: P1-Task#2 | Buffer State: 4/5
[19:51:13.684] [ Consumer-1 ] ITEM CONSUMED      | Item: P3-Task#2 | Buffer State: 3/5
[19:51:13.705] [ Producer-2 ] ITEM INSERTED      | Item: P2-Task#5 | Buffer State: 4/5
[19:51:13.706] [ Producer-2 ] FINISHED           | All designated items produced.
[19:51:13.885] [ Producer-1 ] ITEM INSERTED      | Item: P1-Task#4 | Buffer State: 5/5
[19:51:13.942] [ Consumer-2 ] ITEM CONSUMED      | Item: P2-Task#4 | Buffer State: 4/5
[19:51:13.943] [ Producer-3 ] ITEM INSERTED      | Item: P3-Task#4 | Buffer State: 5/5
[19:51:14.176] [ Consumer-1 ] ITEM CONSUMED      | Item: P3-Task#3 | Buffer State: 4/5
[19:51:14.177] [ Producer-3 ] ITEM INSERTED      | Item: P3-Task#5 | Buffer State: 5/5
[19:51:14.177] [ Producer-3 ] FINISHED           | All designated items produced.
[19:51:14.265] [ Consumer-2 ] ITEM CONSUMED      | Item: P1-Task#3 | Buffer State: 4/5
[19:51:14.265] [ Producer-1 ] ITEM INSERTED      | Item: P1-Task#5 | Buffer State: 5/5
[19:51:14.266] [ Producer-1 ] FINISHED           | All designated items produced.
[19:51:14.483] [ Consumer-1 ] ITEM CONSUMED      | Item: P2-Task#5 | Buffer State: 4/5
[19:51:14.700] [ Consumer-2 ] ITEM CONSUMED      | Item: P1-Task#4 | Buffer State: 3/5
[19:51:14.853] [ Consumer-1 ] ITEM CONSUMED      | Item: P3-Task#4 | Buffer State: 2/5
[19:51:15.210] [ Consumer-2 ] ITEM CONSUMED      | Item: P3-Task#5 | Buffer State: 1/5
[19:51:15.345] [ Consumer-1 ] ITEM CONSUMED      | Item: P1-Task#5 | Buffer State: 0/5
[19:51:15.576] [ Consumer-1 ] FINISHED           | Consumer terminated cleanly.
[19:51:15.590] [ Consumer-2 ] FINISHED           | Consumer terminated cleanly.
===========================================================================
Execution completed in 2.84 seconds.
Final Buffer State: 0 items remaining.
===========================================================================
```