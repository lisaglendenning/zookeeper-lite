package org.apache.zookeeper;

import org.apache.zookeeper.util.AutomataState;

// TODO
public interface SessionConnection {

    public static enum State implements AutomataState<State> {
        ANONYMOUS {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || (nextState == CONNECTING) || (nextState == ERROR);
            }
        },
        CONNECTING {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || CONNECTED.validTransition(nextState);
            }
        },
        CONNECTED {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || DISCONNECTING.validTransition(nextState);
            }
        },
        DISCONNECTING {
            @Override
            public boolean validTransition(State nextState) {
                return super.validTransition(nextState)
                        || DISCONNECTED.validTransition(nextState)
                        || ERROR.validTransition(nextState);
            }
        },
        DISCONNECTED {
            @Override
            public boolean isTerminal() {
                return true;
            }
        },
        ERROR {
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
}
