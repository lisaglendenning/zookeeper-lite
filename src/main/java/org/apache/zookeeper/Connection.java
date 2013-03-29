package org.apache.zookeeper;

import java.net.SocketAddress;

import org.apache.zookeeper.util.AutomataState;
import org.apache.zookeeper.util.Eventful;

import com.google.common.util.concurrent.ListenableFuture;

public interface Connection extends Eventful {
    
    public static enum State implements AutomataState<State> {
        OPENING {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || OPENED.validTransition(nextState);
            }
        }, OPENED {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || CLOSING.validTransition(nextState);
            }
        }, CLOSING {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || CLOSED.validTransition(nextState);
            }
        }, CLOSED {
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
        public State initial() {
            return OPENING;
        }
        
        @Override
        public boolean validTransition(State nextState) {
            return (this == nextState);
        }
    }
    
    State state();

    SocketAddress localAddress();
    SocketAddress remoteAddress();

    ListenableFuture<Void> send(Object message);

    ListenableFuture<Void> flush();

    ListenableFuture<Void> close();
    
    void read();
}
