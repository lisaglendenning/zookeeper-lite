package edu.uw.zookeeper;

import java.net.SocketAddress;


import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.util.AutomataState;
import edu.uw.zookeeper.util.Eventful;

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

    State state();

    SocketAddress localAddress();

    SocketAddress remoteAddress();

    void read();

    <T> ListenableFuture<T> send(T message);

    ListenableFuture<Connection> flush();

    ListenableFuture<Connection> close();
}
