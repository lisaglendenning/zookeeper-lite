package edu.uw.zookeeper;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Maps;

public class EventSink {

    protected ConcurrentMap<Class<? extends Object>, BlockingQueue<Object>> events = Maps
            .newConcurrentMap();

    public boolean isEmpty() {
        return size() == 0;
    }

    public int size() {
        // Note that this is not an atomic operation
        int size = 0;
        for (BlockingQueue<Object> q : events.values()) {
            size += q.size();
        }
        return size;
    }

    public void put(Object event) throws InterruptedException {
        put(event.getClass(), event);
    }

    public void put(Class<? extends Object> type, Object event)
            throws InterruptedException {
        BlockingQueue<Object> q = getQueue(type);
        q.put(event);
    }

    @SuppressWarnings("unchecked")
    public <T> T take(Class<? super T> type) throws InterruptedException {
        BlockingQueue<Object> q = getQueue(type);
        return (T) q.take();
    }

    public BlockingQueue<Object> getQueue(Class<? extends Object> type) {
        if (events.get(type) == null) {
            events.putIfAbsent(type, new LinkedBlockingQueue<Object>());
        }
        return events.get(type);
    }

    @Override
    public String toString() {
        return events.toString();
    }
}
