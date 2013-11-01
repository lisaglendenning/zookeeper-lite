package edu.uw.zookeeper.protocol;

import com.google.common.base.Function;
import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Automaton;


public enum ProtocolState implements Function<ProtocolState, Optional<Automaton.Transition<ProtocolState>>> {

    ANONYMOUS {
        @Override
        public Optional<Automaton.Transition<ProtocolState>> apply(ProtocolState input) {
            switch (input) {
            case ANONYMOUS:
                return Optional.absent();
            case CONNECTING:
            case ERROR:
                return Optional.of(Automaton.Transition.<ProtocolState>create(this, input));
            default:
                throw new IllegalArgumentException(String.valueOf(input));
            }
        }
    },
    CONNECTING {
        @Override
        public Optional<Automaton.Transition<ProtocolState>> apply(ProtocolState input) {
            switch (input) {
            case CONNECTING:
                return Optional.absent();
            case CONNECTED:
            case ERROR:
                return Optional.of(Automaton.Transition.<ProtocolState>create(this, input));
            default:
                throw new IllegalArgumentException(String.valueOf(input));
            }   
        }
    },
    CONNECTED {
        @Override
        public Optional<Automaton.Transition<ProtocolState>> apply(ProtocolState input) {
            switch (input) {
            case CONNECTING:
            case CONNECTED:
                return Optional.absent();
            case DISCONNECTING:
            case ERROR:
            case DISCONNECTED:
                return Optional.of(Automaton.Transition.<ProtocolState>create(this, input));
            default:
                throw new IllegalArgumentException(String.valueOf(input));
            }   
        }
    },
    DISCONNECTING {
        @Override
        public Optional<Automaton.Transition<ProtocolState>> apply(ProtocolState input) {
            switch (input) {
            case CONNECTING:
            case CONNECTED:
            case DISCONNECTING:
                return Optional.absent();
            case ERROR:
            case DISCONNECTED:
                return Optional.of(Automaton.Transition.<ProtocolState>create(this, input));
            default:
                throw new IllegalArgumentException(String.valueOf(input));
            }   
        }
    },
    DISCONNECTED {
        public Optional<Automaton.Transition<ProtocolState>> apply(ProtocolState input) {
            switch (input) {
            case CONNECTING:
            case CONNECTED:
            case DISCONNECTING:
            case DISCONNECTED:
            case ERROR:
                return Optional.absent();
            default:
                throw new IllegalArgumentException(String.valueOf(input));
            }              
        }
    },
    ERROR {
        public Optional<Automaton.Transition<ProtocolState>> apply(ProtocolState input) {
            switch (input) {
            case DISCONNECTING:
            case DISCONNECTED:
                return Optional.of(Automaton.Transition.<ProtocolState>create(this, input));
            default:
                return Optional.absent();
            }                              
        }
    };
}
