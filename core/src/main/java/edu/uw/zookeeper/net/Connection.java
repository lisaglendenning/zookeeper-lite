package edu.uw.zookeeper.net;

import java.net.SocketAddress;
import java.util.concurrent.Executor;


import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Publisher;

/**
 * Asynchronous communication channel.
 * 
 * Posts at least the following events:
 * <ul>
 * <li> Connection.State when the Connection.State changes
 * <li> O when an O is received
 * </ul>
 * 
 * Currently there is only one implementation that is based on Netty 4.0.
 * 
 * @see edu.uw.zookeeper.netty
 */
public interface Connection<I> extends Publisher, Executor {

    public static enum State implements Function<State, Optional<State>> {
        CONNECTION_OPENING {
            @Override
            public Optional<State> apply(State nextState) {
                switch (nextState) {
                case CONNECTION_OPENING:
                    return Optional.absent();
                default:
                    return Optional.of(nextState);
                }
            }
        },
        CONNECTION_OPENED {
            @Override
            public Optional<State> apply(State nextState) {
                switch (nextState) {
                case CONNECTION_CLOSING:
                case CONNECTION_CLOSED:
                    return Optional.of(nextState);
                default:
                    return Optional.absent();
                }
            }
        },
        CONNECTION_CLOSING {
            @Override
            public Optional<State> apply(State nextState) {
                switch (nextState) {
                case CONNECTION_CLOSED:
                    return Optional.of(nextState);
                default:
                    return Optional.absent();
                }
            }
        },
        CONNECTION_CLOSED {
            @Override
            public Optional<State> apply(State nextState) {
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
     */
    void read();

    /**
     * Asynchronously send a message.
     * 
     * @param message I to send
     * @return ListenableFuture that returns {@code message}
     */
    <T extends I> ListenableFuture<T> write(T message);
    
    /**
     * Trigger a flush of written messages.
     */
    void flush();

    /**
     * Transition Connection to CONNECTION_CLOSING.
     * 
     * No-op if connection is already closing or closed.
     * 
     * @return ListenableFuture that returns this
     */
    ListenableFuture<? extends Connection<I>> close();
}
