package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.protocol.proto.OpCode;


public enum ProtocolMessageAutomaton implements Automaton<ProtocolState, Message> {

    ANONYMOUS {
        @Override
        public ProtocolState state() {
            return ProtocolState.ANONYMOUS;
        }
        
        @Override
        public Optional<Automaton.Transition<ProtocolState>> apply(Message input) {
            if (input instanceof ConnectMessage.Request) {
                return Optional.of(Automaton.Transition.create(state(), CONNECTING.state()));
            } else if (input instanceof Message.Anonymous) {
                // no-op
            } else {
                throw new IllegalArgumentException(String.valueOf(input));
            }
            return Optional.absent();
        }
    },
    CONNECTING {
        @Override
        public ProtocolState state() {
            return ProtocolState.CONNECTING;
        }
        
        @Override
        public Optional<Automaton.Transition<ProtocolState>> apply(Message input) {
            if (input instanceof ConnectMessage.Response) {
                if (input instanceof ConnectMessage.Response.Valid) {
                    return Optional.of(Automaton.Transition.create(state(), CONNECTED.state()));
                } else {
                    return Optional.of(Automaton.Transition.create(state(), ERROR.state()));
                }
            } else if (input instanceof Message.ClientRequest || input instanceof FourLetterResponse) {
                // no-op                   
            } else {
                throw new IllegalArgumentException(String.valueOf(input));
            }
            return Optional.absent();
        }
    },
    CONNECTED {
        @Override
        public ProtocolState state() {
            return ProtocolState.CONNECTED;
        }
        
        @Override
        public Optional<Automaton.Transition<ProtocolState>> apply(Message input) {
            if (input instanceof Message.ClientRequest) {
                if (OpCode.CLOSE_SESSION == ((Message.ClientRequest<?>) input).record().opcode()) {
                    return Optional.of(Automaton.Transition.create(state(), DISCONNECTING.state()));
                }
            } else if (input instanceof Message.ServerResponse) {
                // noop
            } else {
                throw new IllegalArgumentException(String.valueOf(input));                    
            }
            return Optional.absent();
        }
    },
    DISCONNECTING {
        @Override
        public ProtocolState state() {
            return ProtocolState.DISCONNECTING;
        }
        
        @Override
        public Optional<Automaton.Transition<ProtocolState>> apply(Message input) {
            if (input instanceof Message.ServerResponse) {
                if (OpCode.CLOSE_SESSION == ((Message.ServerResponse<?>) input).record().opcode()) {
                    return Optional.of(Automaton.Transition.create(state(), DISCONNECTED.state()));
                }
            } else {
                throw new IllegalArgumentException(String.valueOf(input));                    
            }
            return Optional.absent();
        }
    },
    DISCONNECTED {
        @Override
        public ProtocolState state() {
            return ProtocolState.DISCONNECTED;
        }
        
        public Optional<Automaton.Transition<ProtocolState>> apply(Message input) {
            throw new IllegalArgumentException(String.valueOf(input));                    
        }
    },
    ERROR {
        @Override
        public ProtocolState state() {
            return ProtocolState.ERROR;
        }
        
        public Optional<Automaton.Transition<ProtocolState>> apply(Message input) {
            if (input instanceof Message.ClientRequest) {
                if (OpCode.CLOSE_SESSION == ((Message.ClientRequest<?>) input).record().opcode()) {
                    return Optional.of(Automaton.Transition.create(state(), DISCONNECTING.state()));
                }
            } else if (input instanceof Message.ServerResponse) {
                if (OpCode.CLOSE_SESSION == ((Message.ServerResponse<?>) input).record().opcode()) {
                    return Optional.of(Automaton.Transition.create(state(), DISCONNECTED.state()));
                }
            }
            throw new IllegalArgumentException(String.valueOf(input));    
        }
    };
    
    private static final Map<ProtocolState, ProtocolMessageAutomaton> forState = 
            ImmutableMap.copyOf(
                    Maps.uniqueIndex(Arrays.asList(values()), 
                            new Function<ProtocolMessageAutomaton, ProtocolState>() {
                                @Override
                                public ProtocolState apply(
                                        ProtocolMessageAutomaton input) {
                                    return input.state();
                                }
                    }));
    
    public static ProtocolMessageAutomaton forState(ProtocolState state) {
        return forState.get(state);
    }
    
    public static Automaton<ProtocolState, Object> asAutomaton() {
        return asAutomaton(ProtocolState.ANONYMOUS);
    }

    public static Automaton<ProtocolState, Object> asAutomaton(ProtocolState state) {
        return new SimpleAutomaton(forState(state));
    }
    
    /**
     * Not thread-safe
     */
    protected static final class SimpleAutomaton implements Automaton<ProtocolState, Object> {
        
        private ProtocolMessageAutomaton instance;
        
        public SimpleAutomaton(ProtocolMessageAutomaton instance) {
            this.instance = checkNotNull(instance);
        }

        @Override
        public ProtocolState state() {
            return instance.state();
        }

        @Override
        public Optional<Automaton.Transition<ProtocolState>> apply(Object input) {
            Optional<Automaton.Transition<ProtocolState>> output;
            if (input instanceof Message) { 
                output = instance.apply((Message) input);
            } else if (input instanceof ProtocolState) {
                output = instance.state().apply((ProtocolState) input);
            } else {
                throw new IllegalArgumentException(String.valueOf(input));
            }
            if (output.isPresent()) {
                ProtocolState next = output.get().to();
                if (next != instance.state()) {
                    instance = forState(next);
                }
            }
            return output;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(Automaton.class).add("state", instance).toString();
        }
    }
}
