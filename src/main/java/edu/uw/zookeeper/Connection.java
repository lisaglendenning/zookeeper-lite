package edu.uw.zookeeper;

import java.net.SocketAddress;


import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.util.AutomataState;
import edu.uw.zookeeper.util.Eventful;

/**
 * Asynchronous communication channel.
 * 
 * Posts at least the following events:
 * <ul>
 * <li> ConnectionStateEvent when the Connection State changes
 * <li> ConnectionMessageEvent when a message is received
 * </ul>
 * 
 * Currently there is only one implementation that is based on Netty 4.0.
 * 
 * @see edu.uw.zookeeper.netty
 */
public interface Connection extends Eventful {

    public static enum State implements AutomataState<State> {
        CONNECTION_OPENING {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || CONNECTION_OPENED.validTransition(nextState);
            }
        },
        CONNECTION_OPENED {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || CONNECTION_CLOSING.validTransition(nextState);
            }
        },
        CONNECTION_CLOSING {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || CONNECTION_CLOSED.validTransition(nextState);
            }
        },
        CONNECTION_CLOSED {
            @Override
            public boolean isTerminal() {
                return true;
            }
        };

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean validTransition(State nextState) {
            return (this == nextState);
        }
    }

    /**
     * Get current state.
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
     * Schedule a read.
     */
    void read();

    /**
     * Schedule a message to be sent
     * 
     * @param message message to send
     * @return ListenableFuture that returns <code>message</code>
     */
    <T> ListenableFuture<T> send(T message);

    /**
     * Schedule a flush of pending messages.
     * 
     * @return ListenableFuture that returns <code>this</code>
     */
    ListenableFuture<Connection> flush();

    /**
     * Transition Connection to CONNECTION_CLOSING.
     * 
     * No-op if connection is closing or closed.
     * 
     * @return ListenableFuture that returns <code>this</code>
     */
    ListenableFuture<Connection> close();
}
