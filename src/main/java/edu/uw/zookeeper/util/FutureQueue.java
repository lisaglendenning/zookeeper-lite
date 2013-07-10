package edu.uw.zookeeper.util;

import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ForwardingQueue;

public class FutureQueue<V extends Future<?>> extends ForwardingQueue<V> {

    public static <V extends Future<?>> FutureQueue<V> create() {
        return create(new LinkedBlockingQueue<V>());
    }

    public static <V extends Future<?>> FutureQueue<V> create(Queue<V> delegate) {
        return new FutureQueue<V>(delegate);
    }

    protected final Queue<V> delegate;
    
    public FutureQueue(Queue<V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean isEmpty() {
        return peek() == null;
    }
    
    @Override
    public V peek() {
        V next = super.peek();
        if ((next != null) && next.isDone()) {
            return next;
        } else {
            return null;
        }
    }
    
    @Override
    public synchronized V poll() {
        V next = peek();
        if (next != null) {
            return super.poll();
        } else {
            return null;
        }
    }
    
    @Override
    public synchronized void clear() {
        V next;
        while ((next = super.poll()) != null) {
            next.cancel(true);
        }
    }
    
    @Override
    public Queue<V> delegate() {
        return delegate;
    }
}
