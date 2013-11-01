package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.Stateful;
import edu.uw.zookeeper.net.Connection;

public class IntraVmEndpoint<I,O> extends AbstractIntraVmEndpoint<I,O,I,O> implements Stateful<Connection.State>, Eventful<Connection.Listener<? super O>>, Executor {

    public static <I,O> IntraVmEndpoint<I,O> newInstance(
            SocketAddress address,
            Executor executor) {
        return new IntraVmEndpoint<I,O>(
                address,
                LogManager.getLogger(IntraVmEndpoint.class),
                executor, 
                IntraVmPublisher.<O>weakSubscribers());
    }
    
    protected IntraVmEndpoint(
            SocketAddress address,
            Logger logger,
            Executor executor,
            IntraVmPublisher<O> publisher) {
        super(address, logger, executor, publisher);
    }
    
    @Override
    public <T extends I> ListenableFuture<T> write(T message, AbstractIntraVmEndpoint<?,?,?,? super I> remote) {
        if (state().compareTo(Connection.State.CONNECTION_CLOSING) < 0) {
            EndpointWrite<T> task = new EndpointWrite<T>(remote, message, LoggingPromise.create(logger, SettableFuturePromise.<T>create()));
            execute(task);
            return task;
        } else {
            return Futures.immediateFailedFuture(new ClosedChannelException());
        }
    }

    @Override
    public boolean read(O message) {
        if (state().compareTo(Connection.State.CONNECTION_CLOSING) < 0) {
            execute(new EndpointRead(message));
            return true;
        }
        return false;
    }
    
    public class EndpointWrite<T extends I> extends AbstractEndpointWrite<T> implements Runnable {

        public EndpointWrite(
                AbstractIntraVmEndpoint<?,?,?,? super I> remote,
                T message,
                Promise<T> promise) {
            super(remote, message, promise);
        }
        
        @Override
        public Optional<T> call() {
            if (state() != Connection.State.CONNECTION_CLOSED) {
                if (remote.read(task)) {
                    return Optional.of(task);
                } else {
                    close();
                    setException(new ClosedChannelException());
                }
            } else {
                setException(new ClosedChannelException());
            }
            return Optional.absent();
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(getClass()).add("task", task).add("endpoint", remote).toString();
        }
    }

    public class EndpointRead extends AbstractEndpointRead implements Runnable {

        public EndpointRead(O message) {
            super(message);
        }
        
        @Override
        protected void doRead() {
            publisher.handleConnectionRead(get());
        }
    }
}