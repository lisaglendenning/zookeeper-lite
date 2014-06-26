package edu.uw.zookeeper.net.intravm;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.Stateful;
import edu.uw.zookeeper.net.Connection;

public class IntraVmEndpoint<I,O> extends AbstractIntraVmEndpoint<I,O,I,O> implements Stateful<Connection.State>, Eventful<Connection.Listener<? super O>>, Executor {

    public static <I,O> IntraVmEndpoint<I,O> newInstance(
            SocketAddress address,
            Executor executor) {
        Logger logger = LogManager.getLogger(IntraVmEndpoint.class);
        return new IntraVmEndpoint<I,O>(
                address,
                executor, 
                logger,
                IntraVmPublisher.<O>defaults(executor, logger));
    }
    
    protected IntraVmEndpoint(
            SocketAddress address,
            Executor executor,
            Logger logger,
            IntraVmPublisher<O> publisher) {
        super(address, executor, logger, publisher);
    }
    
    @Override
    public <T extends I> ListenableFuture<T> write(T message, AbstractIntraVmEndpoint<?,?,?,? super I> remote) {
        if (state().compareTo(Connection.State.CONNECTION_CLOSING) < 0) {
            CallablePromiseTask<EndpointWrite<T>,T> task = CallablePromiseTask.create(
                    new EndpointWrite<T>(remote, message), 
                    SettableFuturePromise.<T>create());
            LoggingFutureListener.listen(logger, task);
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
    
    public class EndpointWrite<T extends I> extends AbstractEndpointWrite<T> {

        public EndpointWrite(
                AbstractIntraVmEndpoint<?,?,?,? super I> remote,
                T message) {
            super(remote, message);
        }
        
        @Override
        public Optional<T> call() throws IOException {
            if (state() != Connection.State.CONNECTION_CLOSED) {
                if (remote.read(message)) {
                    return Optional.of(message);
                } else {
                    close();
                    throw new ClosedChannelException();
                }
            } else {
                throw new ClosedChannelException();
            }
        }
    }

    public class EndpointRead extends AbstractEndpointRead implements Runnable {

        public EndpointRead(O message) {
            super(message);
        }
        
        @Override
        protected void doRead() {
            publisher.send(get());
        }
    }
}