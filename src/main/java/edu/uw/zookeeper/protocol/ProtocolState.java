package edu.uw.zookeeper.protocol;

import com.google.common.base.Function;
import com.google.common.base.Optional;


public enum ProtocolState implements Function<Message, Optional<ProtocolState>> {

    ANONYMOUS {
        @Override
        public Optional<ProtocolState> apply(Message input) {
            if (input instanceof OpCreateSession.Request) {
                return Optional.of(CONNECTING);
            } else if (input instanceof FourLetterRequest || input instanceof FourLetterResponse) {
                // no-op
            } else {
                throw new IllegalArgumentException(input.toString());
            }
            return Optional.absent();
        }
    },
    CONNECTING {
        @Override
        public Optional<ProtocolState> apply(Message input) {
            if (input instanceof OpCreateSession.Response) {
                if (input instanceof OpCreateSession.Response.Valid) {
                    return Optional.of(CONNECTED);
                } else {
                    return Optional.of(ERROR);
                }
            } else {
                // no-op                   
            }
            return Optional.absent();
        }
    },
    CONNECTED {
        @Override
        public Optional<ProtocolState> apply(Message input) {
            if (input instanceof Operation.SessionRequest) {
                Operation.Request request = ((Operation.SessionRequest)input).request();
                if (request.opcode() == OpCode.CLOSE_SESSION) {
                    return Optional.of(DISCONNECTING);
                }
            } else if (input instanceof Message.SessionMessage) {
                // noop
            } else {
                throw new IllegalArgumentException(input.toString());                    
            }
            return Optional.absent();
        }
    },
    DISCONNECTING {
        @Override
        public Optional<ProtocolState> apply(Message input) {
            if (input instanceof Operation.SessionReply) {
                Operation.Reply reply = ((Operation.SessionReply)input).reply();
                if (reply instanceof Operation.Response) {
                    if (((Operation.Response)reply).opcode() == OpCode.CLOSE_SESSION) {
                        return Optional.of(DISCONNECTED);
                    }
                }
            } else if (input instanceof Message.SessionMessage) {
                // noop
            } else {
                throw new IllegalArgumentException(input.toString());                    
            }
            return Optional.absent();
        }
    },
    DISCONNECTED {
        public Optional<ProtocolState> apply(Message input) {
            throw new IllegalArgumentException(input.toString());                    
        }
    },
    ERROR {
        public Optional<ProtocolState> apply(Message input) {
            throw new IllegalArgumentException(input.toString());                    
        }
    };
}
