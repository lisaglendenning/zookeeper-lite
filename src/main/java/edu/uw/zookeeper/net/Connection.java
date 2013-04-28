package edu.uw.zookeeper.net;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.net.SocketAddress;


import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.util.Eventful;

/**
 * Asynchronous communication channel.
 * 
 * Posts at least the following events:
 * <ul>
 * <li> ConnectionStateEvent when the Connection.State changes
 * <li> ConnectionBufferEvent when there are bytes to be read
 * </ul>
 * 
 * Currently there is only one implementation that is based on Netty 4.0.
 * 
 * @see edu.uw.zookeeper.netty
 */
public interface Connection extends Eventful {

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
     * Get the ByteBufAllocator associated with this connection.
     * 
     * @return allocator
     */
    ByteBufAllocator allocator();
    
    /**
     * Trigger a read.
     * 
     * Triggers a ConnectionBufferEvent if there are bytes to be read.
     */
    void read();

    /**
     * Asynchronously send a Buf.
     * 
     * @param buf Buf to send
     * @return ListenableFuture that returns {@code buf}
     */
    ListenableFuture<ByteBuf> write(ByteBuf buf);

    /**
     * Asynchronously flush pending writes.
     * 
     * @return ListenableFuture that returns this
     */
    ListenableFuture<Connection> flush();

    /**
     * Transition Connection to CONNECTION_CLOSING.
     * 
     * No-op if connection is already closing or closed.
     * 
     * @return ListenableFuture that returns this
     */
    ListenableFuture<Connection> close();
}
