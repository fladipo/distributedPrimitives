package DistributedPrimitives;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A high performance, thread-safe FIFO Bounded Blocking Queue designed for back
 * pressure management and
 * resource governance in high-scale ingestion pipelines. This queue forces
 * producers to block when the
 * queue is full and consumers to block when it is empty.
 * 
 */
public class BoundedBlockingQueue<T> {
    private final Deque<T> queue;

    // Do we need a fairness policy for the lock? If multiple producers are waiting,
    // should the one who has been waiting the
    // longest get the next slot? ReentrantLock(true) ensures no thread starves, but
    // it has a massive thoughput penalty because
    // it forces more complex thread scheduling. In high scale data platforms, we
    // usually prefer unfair locks for max performance
    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();

    // Since we have multiple producers and consumers, we must use signal() and
    // notify() more carefully. If we have many consumers, we need to ensure our
    // signaling logic does not cause a thundering herd where everyone wakes up
    private final Condition notFull = lock.newCondition(); // The producer is waiting for the state to become not full.
    private final Condition notEmpty = lock.newCondition(); // The consumer is waiting for the state to become not
                                                            // empty.

    public BoundedBlockingQueue(int capacity) {
        this.capacity = capacity;

        // ArrayDeque is backed by a contiguous array, while LinkedList is composed of
        // scattered node objects.
        // Since array object references are stored next to each other, the CPU can
        // prefetch them into its cache, causing fewer cache misses since
        // navigating through a LinkedList requires following pointers to random memory
        // addresses in the heap.
        queue = new ArrayDeque<>(capacity);
    }

    /**
     * Enqueues an item, blocking indefinitely if full
     * 
     * @param item The element to add to the queue
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void enqueue(T item) throws InterruptedException {
        if (item == null)
            throw new NullPointerException("Null items are not allowed...");

        lock.lock();
        try {
            // Why a while loop? The thread wakes up, goes back to the top of the loop,
            // checks the condition again, sees it's full
            // and immediately goes back to sleep
            while (queue.size() == this.capacity) {
                System.out.println("Producer thread is waiting inside the conditional loop.");

                // await is infinite so the thread is waiting forever. While this is a great
                // place to start, its not ideal for production code. An alternative
                // would be to have a timeout to prevent thread starvation.
                notFull.await();
            }

            queue.offer(item);
            // Wake up one consumer thread, preserving CPU cycles
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Dequeues an item, blocking indefinitely if the queue is empty
     * 
     * @return The element removed from the head of the queue.
     * @throws InterruptedException if the thread is interrupted while waiting.
     */
    public T dequeue() throws InterruptedException {
        lock.lock();
        try {
            while (queue.isEmpty()) {
                System.out.println("Consumer thread is waiting inside the conditional loop.");
                notEmpty.await(); // put this thread to sleep since the queue is empty
            }

            T item = queue.poll();
            notFull.signal(); // wake up a producer
            return item;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Safe internal size lookup for thread safety and to prevent
     * reading corrupted data
     */
    public int size() {
        // To enforce concurrency, we can use an AtomicInteger instead that
        // allows for volatile reads, allowing for lock-free reads.
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        BoundedBlockingQueue<Integer> queue = new BoundedBlockingQueue<>(2);

        // Validate blocking on empty queue
        Thread consumer = Thread.ofVirtual().start(() -> {
            try {
                System.out.println("Consumer: Waiting ....");
                Integer val = queue.dequeue();
                System.out.println("Consumer: Got " + val);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Force main thread to sleep so that the consumer thread can enter the while
        // loop
        // otherwise we have a race condition
        Thread.sleep(100);
        queue.enqueue(42);

        // Without the join, the main thread will finish its execution and
        // exit before the virtual thread has a chance to do its work.
        consumer.join();

        // Verify that when the queue is full, we block
        queue.enqueue(15);
        queue.enqueue(33); // At capacity

        Thread producer = Thread.ofVirtual().start(() -> {
            try {
                System.out.println("Producer: Trying to add 29");
                queue.enqueue(29);
                System.out.println("Producer: Added 29");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread.sleep(100);
        queue.dequeue();
        producer.join();
    }
}