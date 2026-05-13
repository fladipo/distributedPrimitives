package DistributedPrimitives;

import java.util.*;

/**
 * A memory-efficient event aggregator that merges multiple chronological streams into a single globally sorted consumer.
 * 
 * Implementation: Use a min-heap (PriorityQueue) to ensure that the heap size scales with the number of producers (k), not the
 * total volume of events (N). This ensures a fixed memory footprint regardless of stream duration.
 */
public class MergingEventStream implements Iterator<MergingEventStream.Event> {
    public static class Event {
        final long timestamp;
        final String id;
        final String payload;

        public Event(long timestamp, String id, String payload) {
            this.timestamp = timestamp;
            this.id = id;
            this.payload = payload;
        }
    }

    private static class ProducerNode {
        Event event;
        Iterator<Event> it;

        ProducerNode(Event event, Iterator<Event> it) {
            this.event = event;
            this.it = it;
        }
    }

    private final PriorityQueue<ProducerNode> mergedStream;

    /**
     * Initializes the aggregator with k sorted event streams. 
     * @param producers
     */
    public MergingEventStream(List<Iterator<Event>> producers) {
  
        this.mergedStream = new PriorityQueue<>(Math.max(1, producers.size()), (a,b) -> Long.compare(a.event.timestamp, b.event.timestamp));

        for (Iterator<Event> it : producers) {
            if (it != null && it.hasNext()) {
                Event firstEvent = it.next();
                if (firstEvent != null)  {
                    mergedStream.offer(new ProducerNode(firstEvent, it));
                }
            }
        }
    }

    /**
     * Checks if any events remain in the producer streams
     */
    @Override
    public boolean hasNext() {
        return !mergedStream.isEmpty();
    }

    /**
     * Polls the globally earliest event and refills the heap from the same producer
     */
    @Override
    public Event next() {
        if (!hasNext()) throw new NoSuchElementException();

        // Get the next smallest element
        ProducerNode smallestElement = mergedStream.poll();
        Iterator<Event> it = smallestElement.it;
        if (it.hasNext()) {
            mergedStream.offer(new ProducerNode(it.next(), it));
        }

        return smallestElement.event;
    }

    public static void main(String[] args) {
        // Create the list of producers
        Iterator<Event> producer1 = List.of(
            new Event(1, "id1", "data"), 
            new Event(3, "id3", "data")).iterator();

        Iterator<Event> producer2 = List.of(
            new Event(2, "id2", "data"), 
            new Event(4, "id4", "data")).iterator();

        List<Iterator<Event>> producers = List.of(producer1, producer2);
        MergingEventStream aggregator = new MergingEventStream(producers);

        while(aggregator.hasNext()) {
            Event event = aggregator.next();
            System.out.println("The timestamp is " +  event.timestamp + " for " + event.id);
        }
    }
}