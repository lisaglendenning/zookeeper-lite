package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.Factories;
import edu.uw.zookeeper.common.Stateful;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.LoggingMarker;

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

    public abstract <T extends I> ListenableFuture<T> write(T message, AbstractIntraVmEndpoint<?,?,?,? super U> remote);

    public abstract boolean read(V message);
    
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
        return Objects.toStringHelper(this)
                .add("address", address)
                .add("state", state()).toString();
    }

    public abstract class AbstractEndpointRead extends Factories.Holder<V> implements Runnable {

        public AbstractEndpointRead(V message) {
            super(message);
        }
        
        @Override
        public void run() {
            // we want to make sure that we don't post any messages
            // after declaring that we are closed
            Connection.State connectionState = state.state();
            switch (connectionState) {
            case CONNECTION_OPENING:
            {
                Connection.State nextState = Connection.State.CONNECTION_OPENED;
                if (state.apply(nextState).isPresent()) {
                    publisher.send(
                            Automaton.Transition.create(connectionState, nextState));
                }
            }
            case CONNECTION_OPENED:
            case CONNECTION_CLOSING:
            {
                doRead();
                break;
            }
            default:
            {
                logger.trace(LoggingMarker.NET_MARKER.get(), "DROPPING {}", this);
                break;
            }
            }
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(getClass()).add("message", get()).add("endpoint", AbstractIntraVmEndpoint.this).toString();
        }
        
        protected abstract void doRead();
    }

    public abstract class AbstractEndpointWrite<T extends I> implements Callable<Optional<T>> {

        protected final AbstractIntraVmEndpoint<?,?,?,? super U> remote;
        protected final T message;
        
        public AbstractEndpointWrite(
                AbstractIntraVmEndpoint<?,?,?,? super U> remote,
                T message) {
            this.message = message;
            this.remote = remote;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(getClass()).add("message", message).add("remote", remote).toString();
        }
    }

    public class EndpointClose implements Runnable {

        public EndpointClose() {}
        
        @Override
        public void run() {
            Connection.State prevState = Connection.State.CONNECTION_OPENED;
            Connection.State nextState = Connection.State.CONNECTION_CLOSING;
            if (state.state() == nextState) {
                publisher.send(
                        Automaton.Transition.create(prevState, nextState));
                
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
            return Objects.toStringHelper(getClass()).add("endpoint", AbstractIntraVmEndpoint.this).toString();
        }
    }
}