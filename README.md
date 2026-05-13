# Distributed Primitives
### Engineering for Reliability and High Throughput

This repository is a curated collection of core systems problems I’ve solved, focusing on core concepts of distributed systems. These aren't just coding exercises but they are the primitives required to build Tier-0 services that can survive retry storms, deadlocks, and high-concurrency contention. 

I have documented my architectural analysis and trade-offs for each one.

---

## **Catalog of Primitives**
1. [Concurrent File Buffer (Telemetry Agent)](#concurrent-file-buffer-telemetry-agent)
2. [Bounded Blocking Queue](#bounded-blocking-queue)
3. [Merging Event Stream](#merging-event-stream)

---

## Concurrent File Buffer (Telemetry Agent)

This example implements a thread-safe, fixed-size buffer designed for high-throughput telemetry ingestion. It bridges the gap between fast in-memory producers and slow disk I/O consumers, specifically solving for deadlock prevention in recursive locking scenarios.

### **The Problem Space**
We are building a telemetry agent that buffers small log events in memory.
1. **Write(data):** Fast. Appends to memory.
2. **Flush():** Slow. Persists to disk.

**The Issue:** If `Write()` fills the buffer, it must call `Flush()`. If both methods are protected by a standard non-reentrant mutex, `Write()` (holding the lock) calls `Flush()` (waiting for the lock), causing the thread to hang forever.

### **Analysis & Trade-offs**

#### **The "Retry Storm" Risk**
In distributed systems (like the NBS Reliability work I led), a deadlocked buffer doesn't just kill one thread. It causes upstream producers to timeout, leading to aggressive retries that can cascade and take down the entire ingestion fleet.

#### **Solution Evaluation**


| Option | Strategy | Trade-off | Decision |
| :--- | :--- | :--- | :--- |
| **Option 1** | **Internal Flush Helper** | Create a private `internalFlush()` that assumes the lock is already held. | **Selected.** Cleanest separation of concerns. |
| **Option 2** | **ReentrantLock** | Use Java's `ReentrantLock` allowing the same thread to re-acquire the lock. | **Selected.** Necessary for safety, even if slightly heavier than a raw mutex. |
| **Option 3** | **Double Buffering** | Swap active buffers so producers don't wait for disk I/O. | **Deferred.** Adds complexity (async coordination) which wasn't required for this specific durability guarantee. |

### **Implementation Details**

#### **1. The Double-Gated Flush**
The `write()` method employs a logic gate:
```java
lock.lock();
if (buffer.full) {
    internalFlush(); // Safe because we already hold the lock
}
```
This pattern ensures atomicity. No other thread can sneak in a write between the buffer becoming full and the flush occurring.

#### **2. Java 21 Virtual Threads (Loom)**
I chose `ReentrantLock` over `synchronized` blocks specifically to support Loom.
*   **Physics:** `synchronized` can pin the virtual thread to the carrier OS thread during blocking operations.
*   **Optimization:** `ReentrantLock` allows the virtual thread to unmount when blocked, maximizing CPU utilization during high-contention `flush()` cycles.

#### **3. Performance Verification**
Included is a High-Contention Stress Test utilizing `Executors.newVirtualThreadPerTaskExecutor()`.
*   **Load:** 2,000+ interleaved `write` and `flush` tasks.
*   **Result:** Successfully prevents deadlocks even when threads fight for the lock millisecond-by-millisecond.

---

## Bounded Blocking Queue

### **The Problem Space**
How do we protect system boundaries when fast producers outpace slow storage components? We need a thread-safe FIFO data structure with a maximum capacity that forces producers to block when full and consumers to block when empty, establishing back pressure.

### **Mitigating Lock Contention**
The risk here is using a single coarse lock that wakes up every sleeping thread during an update. At scale, this triggers high CPU context switching overhead and can cause Thundering Herd spikes. If a consumer downstream hangs, infinite blocking can starve your thread pool.

### **Analysis & Trade-offs**

| Feature | Coarse Lock | Condition-Based wait-set (My Approach) |
| :--- | :--- | :--- |
| **Thread Signaling** | High CPU churn — wakes up all threads concurrently. | **Low CPU churn** — `signal()` wakes up exactly one target worker. |
| **Memory Locality** | Node-based `LinkedList` forces pointer chasing. | **Contiguous `ArrayDeque`** maximizes CPU cache prefetching. |
| **Lock Scheduling** | Fair allocation protects against starvation. | **Unfair allocation** maximizes performance and throughput. |

I decided to utilize a single `ReentrantLock` along with two distinct `Condition` queues (`notFull` and `notEmpty`). By separating threads into separate waiting areas based on state, backing the collection with a contiguous memory `ArrayDeque`, and explicitly avoiding fair scheduling downsides, I eliminate unnecessary wakeups and create speed under heavy ingestion load.

### **Implementation Details**

#### **1. Isolated Condition Wait-Sets**
The signaling model splits thread wait-states into independent condition queues:
```java
private final Condition notFull = lock.newCondition();
private final Condition notEmpty = lock.newCondition();
```
This enables isolated thread targeting. Calling `notEmpty.signal()` directly addresses a single waiting consumer rather than broadcasting to the entire thread pool.

#### **2. Allocation Boundary & Memory Contiguity**
The structure relies on an pre-sized `ArrayDeque` memory buffer:
```java
this.queue = new ArrayDeque<>(capacity);
```
This forces all object tracking onto a contiguous block of memory, allowing the processor to execute cache-line prefetching and eliminating the pointer-chasing overhead found in linked structures.

---

## Merging Event Stream

### **The Problem Space**
In a sharded architecture, telemetry is often generated and stored locally by shard. For downstream analytics, we need to merge these $K$ locally sorted streams from producers into a single, globally sorted view.

### **Fixed Memory Footprint**
The risk here is attempting to load all $N$ events into a global list and sorting them all. For continuous streams, this leads to an inevitable OOM.

### **Analysis & Trade-offs**


| Feature | Naive Sort | Heap-based Merge (My Approach) |
| :--- | :--- | :--- |
| **Space Complexity** | O(N) — Scales with event volume. | **O(K)** — Scales only with producer count. |
| **Latency** | High (must wait for end-of-stream). | **Low** (can stream first event immediately). |
| **Suitability** | Offline batch jobs | **Real-time Ingestion** |

I decided to utilize a Min-Heap to track the head of each stream. By always polling the smallest timestamp and immediately refilling from the same source, we guarantee a deterministic order while strictly limiting memory usage to $K$ event nodes.

### **Implementation Details**

#### **1. Bounded Heap**
The aggregation engine limits its heap allocation strictly to the total number of live producer channels ($K$):
```java
this.mergedStream = new PriorityQueue<>(Math.max(1, producers.size()), (a,b) -> Long.compare(a.event.timestamp, b.event.timestamp));
```
By restricting internal state tracking to a single active event per shard node, memory capacity scales independent of total event depth ($N$).

#### **2. Deterministic Stream Refilling**
The consumer pipeline refills the priority queue exclusively from the shard that produced the earliest chronological token:
```java
ProducerNode smallestElement = mergedStream.poll();
    Iterator<Event> it = smallestElement.it;
    if (it.hasNext()) {
        mergedStream.offer(new ProducerNode(it.next(), it));
}
```
This keeps our event ordering completely accurate when reading data, without slowing things down when multiple shards drop out-of-order logs into the pipeline simultaneously.

---
