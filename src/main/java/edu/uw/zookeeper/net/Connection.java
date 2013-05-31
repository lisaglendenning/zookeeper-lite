package edu.uw.zookeeper.net;

import java.net.SocketAddress;
import java.util.concurrent.Executor;


import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.protocol.Codec;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

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
    
    public static interface CodecFactory<I,O,C extends Connection<I>> extends ParameterizedFactory<Connection<I>,  Pair<C, ? extends Codec<? super I, Optional<? extends O>>>> {}

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
     * Triggers a ConnectionBufferEvent if there are bytes to be read.
     */
    void read();

    /**
     * Asynchronously send a message.
     * 
     * @param message I to send
     * @return ListenableFuture that returns {@code message}
     */
    ListenableFuture<I> write(I message);

    /**
     * Asynchronously flush pending writes.
     * 
     * @return ListenableFuture that returns this
     */
    ListenableFuture<Connection<I>> flush();

    /**
     * Transition Connection to CONNECTION_CLOSING.
     * 
     * No-op if connection is already closing or closed.
     * 
     * @return ListenableFuture that returns this
     */
    ListenableFuture<Connection<I>> close();
}
