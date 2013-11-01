package edu.uw.zookeeper.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.AbstractPair;

public abstract class ConnectionTestAdapter {
    
    public static class QueuedConnection<I,O,T extends Connection<? super I,? extends O,?>> extends AbstractPair<T, QueueingConnectionListener<O>> {

        public static <I,O,T extends Connection<? super I,? extends O,?>> QueuedConnection<I,O,T> create(T connection, QueueingConnectionListener<O> listener) {
            listener.subscribe(connection);
            return new QueuedConnection<I,O,T>(connection, listener);
        }
        
        public QueuedConnection(T connection, QueueingConnectionListener<O> listener) {
            super(connection, listener);
        }
        
        public T connection() {
            return first;
        }
        
        public QueueingConnectionListener<O> listener() {
            return second;
        }
    }

    protected void pingPong(
            QueuedConnection<? super String,? super String,?> c1, 
            QueuedConnection<? super String,? super String,?> c2) throws InterruptedException, ExecutionException {
        String c1Message = "ping";
        ListenableFuture<?> future = c1.connection().write(c1Message);
        String c2Message = (String) c2.listener().reads().take();
        assertSame(c1Message, future.get());
        assertEquals(c1Message, c2Message);
        
        c2Message = "pong";
        future = c2.connection().write(c2Message);
        c1Message = (String) c1.listener().reads().take();
        assertSame(c2Message, future.get());
        assertEquals(c2Message, c1Message);
    }
}
