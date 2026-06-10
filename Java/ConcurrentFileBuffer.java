package DistributedPrimitives.Java;

import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A high throughput telemery buffer designed to batch small log events before
 * persisting to disk.
 * 
 * Problem Statement: We are building a telemetry agent that buffers small log
 * events in memory before performing
 * an expensive disk write.
 */
public class ConcurrentFileBuffer {
    private final char[] buffer;
    private final int maxCapacity;
    private final ReentrantLock lock;
    private int position = 0;

    public ConcurrentFileBuffer(int maxCapacity) {
        if (maxCapacity <= 0)
            throw new IllegalArgumentException("Capacity must be positive.");
        this.maxCapacity = maxCapacity;
        this.buffer = new char[maxCapacity];
        this.lock = new ReentrantLock();
    }

    /**
     * Appends data to the internal buffer. Triggers a flush if full.
     * 
     * Critical failure mode:
     * If a disk write hangs indefinitely during internalFlush(), any virtual thread
     * waiting at lock.lock()
     * cannot be interrupted. This can cause a silent memory leak of blocked virtual
     * threads.
     * 
     * The Fix: I would use lock.lockInterruptibly() or lock.tryLock(timeout) to
     * allow the system to shed load
     * or cancel stalled tasks gracefully.
     * 
     * @param data
     */
    public void write(char data) {
        lock.lock();
        try {
            if (position == maxCapacity) {
                internalFlush();
            }

            buffer[position++] = data;

        } finally {
            lock.unlock();
        }
    }

    /**
     * Explicitly writes data to disk.
     * 
     * Trade-Off: We use a ReentrantLock combined with a private internalFlush()
     * While a simple mutex might be faster, the ReentrantLock prevents the
     * self-deadlock
     * if we ever need to call Flush() from within a write context.
     */
    public void flush() {
        // Buffer is full, so flush to disk
        lock.lock();
        try {
            internalFlush();

        } finally {
            lock.unlock();
        }
    }

    // We have a private method internalFlush() that performs the actual disk write
    // without acquiring any locks.
    private void internalFlush() {
        if (position == 0)
            return;

        // Simulate disk I/O
        System.out.println("Flushing " + position + " chars to disk");
        position = 0;
    }

    /**
     * Perform a stress test
     * 
     * Validates deadlock freedom using Java 21 Virtual Threads to simulate high
     * contention.
     */
    public static void main(String[] args) {
        ConcurrentFileBuffer buffer = new ConcurrentFileBuffer(100);
        System.out.println("Starting high contention stress test ... ");
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 1000; i++) {
                char data = (char) ('A' + i % 26);
                executor.submit(() -> buffer.write(data));
                executor.submit(() -> buffer.flush());
            }
        }

        System.out.println("We successfully completed without a deadlock");
    }
}