package org.apache.zookeeper;

import org.apache.zookeeper.util.AutomataState;


// TODO
public class SessionConnection {

    public static enum State implements AutomataState<State> {
        ANONYMOUS {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || (nextState == CONNECTING)
                        || (nextState == ERROR);
            }
        }, CONNECTING {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || CONNECTED.validTransition(nextState);
            }
        }, CONNECTED {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || CLOSING.validTransition(nextState);
            }
        }, CLOSING {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || CLOSED.validTransition(nextState)
                        || ERROR.validTransition(nextState);
            }
        }, CLOSED {
            @Override
            public boolean isTerminal() {
                return true;
            }
        }, ERROR {
            @Override
            public boolean isTerminal() {
                return true;
            }
        };

        @Override
        public boolean isTerminal() {
            return false;
        }
        
        public State initial() {
            return ANONYMOUS;
        }
        
        @Override
        public boolean validTransition(State nextState) {
            return (this == nextState);
        }
    }
}
