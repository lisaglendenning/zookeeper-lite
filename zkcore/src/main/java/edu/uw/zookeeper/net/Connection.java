package edu.uw.zookeeper.net;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Eventful;


/**
 * Asynchronous network channel.
 * 
 * @see edu.uw.zookeeper.netty
 * @see edu.uw.zookeeper.net.intravm
 */
public interface Connection<I,O, C extends Connection<I,O,C>> extends Eventful<Connection.Listener<? super O>>, Executor {

    public static interface Listener<O> {
        void handleConnectionState(Automaton.Transition<Connection.State> state);
        void handleConnectionRead(O message);
    }
    
    public static enum State implements Function<State, Optional<Automaton.Transition<State>>> {
        CONNECTION_OPENING {
            @Override
            public Optional<Automaton.Transition<State>> apply(State nextState) {
                switch (nextState) {
                case CONNECTION_OPENING:
                    return Optional.absent();
                default:
                    return Optional.of(Automaton.Transition.<State>create(this, nextState));
                }
            }
        },
        CONNECTION_OPENED {
            @Override
            public Optional<Automaton.Transition<State>> apply(State nextState) {
                switch (nextState) {
                case CONNECTION_CLOSING:
                case CONNECTION_CLOSED:
                    return Optional.of(Automaton.Transition.<State>create(this, nextState));
                default:
                    return Optional.absent();
                }
            }
        },
        CONNECTION_CLOSING {
            @Override
            public Optional<Automaton.Transition<State>> apply(State nextState) {
                switch (nextState) {
                case CONNECTION_CLOSED:
                    return Optional.of(Automaton.Transition.<State>create(this, nextState));
                default:
                    return Optional.absent();
                }
            }
        },
        CONNECTION_CLOSED {
            @Override
            public Optional<Automaton.Transition<State>> apply(State nextState) {
                return Optional.absent();
            }
        };
    }

    /**
     * Get current Connection.State.
     * 
     * @return current state
     */
    State state();

    /**
     * Get local SocketAddress.
     * 
     * @return local SocketAddress
     */
    SocketAddress localAddress();

    /**
     * Get remote SocketAddress.
     * 
     * @return remote SocketAddress
     */
    SocketAddress remoteAddress();

    /**
     * Trigger a read.
     * 
     * @return this
     */
    C read();

    /**
     * Asynchronously send a message.
     * 
     * @param message I to send
     * @return ListenableFuture that returns {@code message}
     */
    <T extends I> ListenableFuture<T> write(T message);
    
    /**
     * Trigger a flush of written messages.
     * 
     * @return this
     */
    C flush();

    /**
     * Transition Connection to CONNECTION_CLOSING.
     * 
     * No-op if connection is already closing or closed.
     * 
     * @return ListenableFuture that returns this
     */
    ListenableFuture<? extends C> close();
}
