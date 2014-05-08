package edu.uw.zookeeper.protocol;

import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.net.Connection;

public abstract class PingTask<I,O,T extends Connection<? super I, ? extends O,?>> extends TimeOutActor<I,Void> implements Connection.Listener<O>{

    protected final I ping;
    protected final WeakReference<T> connection;

    protected PingTask(
            I ping,
            T connection,
            TimeOutParameters parameters,
            ScheduledExecutorService scheduler,
            Set<Pair<Runnable,Executor>> listeners,
            Promise<Void> promise,
            Logger logger) {
        super(parameters, scheduler, listeners, promise, logger);
        this.connection = new WeakReference<T>(connection);
        this.ping = ping;
        
        connection.subscribe(this);
    }

    @Override
    public void handleConnectionState(
            Automaton.Transition<Connection.State> state) {
        switch (state.to()) {
        case CONNECTION_CLOSING:
        case CONNECTION_CLOSED:
            stop();
            break;
        default:
            break;
        }
    }
    
    @Override
    protected synchronized void doRun() {
        // should ping now, or ok to wait a while?
        if ((parameters.getTimeOut() != NO_TIMEOUT) && (parameters.getRemaining() < parameters.getTimeOut()*0.75)) {
            ping();
        }
    }
    
    @Override
    protected synchronized void doStop() {
        T connection = this.connection.get();
        if (connection != null) {
            connection.unsubscribe(this);
        }
        
        super.doStop();
    }
    
    @Override
    protected synchronized long nextTick() {
        // somewhat arbitrary...
        return Math.max(parameters.getRemaining() / 2L, 0L);
    }
    
    @Override   
    protected synchronized Objects.ToStringHelper toStringHelper() {
        return super.toStringHelper().add("connection", connection.get());
    }
    
    protected synchronized void ping() {
        T connection = this.connection.get();
        if ((connection == null) || (connection.state().compareTo(Connection.State.CONNECTION_CLOSING) >= 0)) {
            stop();
        } else {
            logger.trace(LoggingMarker.PING_MARKER.get(), "PING: {}", this);
            parameters.setTouch();
            ListenableFuture<I> future = connection.write((I) ping);
            future.addListener(new PingListener(future), SameThreadExecutor.getInstance());
        }
    }

    protected class PingListener implements Runnable {

        private final ListenableFuture<I> future;
        
        public PingListener(ListenableFuture<I> future) {
            this.future = future;
        }
        
        @Override
        public void run() {
            if (future.isDone()) {
                if (!future.isCancelled()) {
                    try {
                        future.get();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                        promise.setException(e.getCause());
                    }
                } else {
                    stop();
                }
            }
        }
    }
}
