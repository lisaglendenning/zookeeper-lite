package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Actors;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.Stateful;
import edu.uw.zookeeper.net.Connection;

public abstract class AbstractIntraVmEndpoint<I,O,U,V> implements Stateful<Connection.State>, Eventful<Connection.Listener<? super O>>, Executor {
    
    protected final Logger logger;
    protected final Executor executor;
    protected final IntraVmPublisher<O> publisher;
    protected final SocketAddress address;
    protected final Automaton<Connection.State, Connection.State> state;

    /**
     * @param executor execute order must preserve submit order
     */
    protected AbstractIntraVmEndpoint(
            SocketAddress address,
            Executor executor,
            Logger logger,
            IntraVmPublisher<O> publisher) {
        super();
        this.logger = logger;
        this.executor = executor;
        this.address = address;
        this.publisher = publisher;
        this.state = Automatons.createSynchronized(
                Automatons.createSimple(Connection.State.CONNECTION_OPENING));
    }
    
    @Override
    public Connection.State state() {
        return state.state();
    }
    
    public SocketAddress address() {
        return address;
    }
    
    public abstract Actor<V> reader();
    
    public abstract AbstractEndpointWriter writer();

    public boolean close() {
        if (state.apply(Connection.State.CONNECTION_CLOSING).isPresent()) {
            execute(new EndpointClose());
            return true;
        }
        return false;
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public void subscribe(Connection.Listener<? super O> listener) {
        publisher.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(Connection.Listener<? super O> listener) {
        return publisher.unsubscribe(listener);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("address", address)
                .add("state", state()).toString();
    }
    
    public abstract class AbstractEndpointActor<T> extends Actors.ExecutedQueuedActor<T> {

        protected AbstractEndpointActor(Queue<T> mailbox) {
            super(AbstractIntraVmEndpoint.this.executor, mailbox, AbstractIntraVmEndpoint.this.logger);
        }
        
        protected AbstractEndpointActor(Executor executor, Queue<T> mailbox, Logger logger) {
            super(executor, mailbox, logger);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(AbstractIntraVmEndpoint.this).toString();
        }
    }
    
    public abstract class AbstractEndpointReader extends AbstractEndpointActor<V> {

        protected AbstractEndpointReader(Deque<V> mailbox) {
            super(mailbox);
        }
        
        protected Deque<V> mailbox() {
            return (Deque<V>) mailbox;
        }

        @Override
        protected void doRun() {
            Connection.State connectionState = AbstractIntraVmEndpoint.this.state.state();
            switch (connectionState) {
            case CONNECTION_OPENING:
            {
                Connection.State nextState = Connection.State.CONNECTION_OPENED;
                if (AbstractIntraVmEndpoint.this.state.apply(nextState).isPresent()) {
                    publisher.send(
                            Automaton.Transition.create(connectionState, nextState));
                }
                break;
            }
            case CONNECTION_CLOSED:
            {
                // we want to make sure that we don't post any messages
                // after declaring that we are closed
                stop();
                return;
            }
            default:
                break;
            }

            V last = mailbox().peekLast();
            try {
                super.doRun();
            } catch (Exception e) {
                close();
                stop();
                return;
            }
            
            if (last != mailbox().peekLast()) {
                schedule();
            }
        }

        @Override
        protected void runExit() {
            state.compareAndSet(State.RUNNING, State.WAITING);
        }
    }
    
    public abstract class AbstractEndpointWriter extends AbstractEndpointActor<EndpointWrite<? extends I,U>> {

        protected AbstractEndpointWriter(Queue<EndpointWrite<? extends I,U>> mailbox) {
            super(mailbox);
        }
        
        public <T extends I> ListenableFuture<T> submit(T message, AbstractIntraVmEndpoint<?,?,?,? super U> remote) {
            if (AbstractIntraVmEndpoint.this.state().compareTo(Connection.State.CONNECTION_CLOSING) < 0) {
                EndpointWrite<T,U> task = LoggingFutureListener.listen(logger, EndpointWrite.create(message, remote));
                if (!send(task)) {
                    task.cancel(false);
                }
                return task;
            } else {
                return Futures.immediateFailedFuture(new ClosedChannelException());
            }
        }


        @Override
        protected void doRun() {
            if (AbstractIntraVmEndpoint.this.state.state() == Connection.State.CONNECTION_CLOSED) {
                stop();
                return;
            }
            
            try {
                super.doRun();
            } catch (Exception e) {
                close();
                stop();
            }
        }
        
        @Override
        protected void doStop() {
            Exception exception = new ClosedChannelException();
            EndpointWrite<?,?> next;
            while ((next = mailbox.poll()) != null) {
                next.setException(exception);
            }
        }
    }

    public final static class EndpointWrite<T,U> extends ForwardingPromise.SimpleForwardingPromise<T> implements Runnable {

        public static <T,U> EndpointWrite<T,U> create(
                T message,
                AbstractIntraVmEndpoint<?,?,?,? super U> remote) {
            return new EndpointWrite<T,U>(message, remote, SettableFuturePromise.<T>create());
        }
        
        private final AbstractIntraVmEndpoint<?,?,?,? super U> remote;
        private final T message;
        
        public EndpointWrite(
                T message,
                AbstractIntraVmEndpoint<?,?,?,? super U> remote,
                Promise<T> promise) {
            super(promise);
            this.message = message;
            this.remote = remote;
        }
        
        public T message() {
            return message;
        }
        
        public AbstractIntraVmEndpoint<?,?,?,? super U> remote() {
            return remote;
        }

        @Override
        public void run() {
            set(message());
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(message).addValue(remote).addValue(super.toString()).toString();
        }
    }

    public final class EndpointClose implements Runnable {

        public EndpointClose() {}
        
        @Override
        public void run() {
            Connection.State prevState = Connection.State.CONNECTION_OPENED;
            Connection.State nextState = Connection.State.CONNECTION_CLOSING;
            if (state.state() == nextState) {
                publisher.send(
                        Automaton.Transition.create(prevState, nextState));
                reader().stop();
                writer().stop();
                
                prevState = nextState;
                nextState = Connection.State.CONNECTION_CLOSED;
                if (state.apply(nextState).isPresent()) {
                    publisher.send(
                            Automaton.Transition.create(prevState, nextState));
                }
            }
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(AbstractIntraVmEndpoint.this).toString();
        }
    }
}