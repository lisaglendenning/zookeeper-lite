package edu.uw.zookeeper.protocol;

import com.google.common.base.Function;
import com.google.common.base.Optional;

import edu.uw.zookeeper.protocol.proto.OpCode;


public enum ProtocolState implements Function<Message, Optional<ProtocolState>> {

    ANONYMOUS {
        @Override
        public Optional<ProtocolState> apply(Message input) {
            if (input instanceof ConnectMessage.Request) {
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
            if (input instanceof ConnectMessage.Response) {
                if (input instanceof ConnectMessage.Response.Valid) {
                    return Optional.of(CONNECTED);
                } else {
                    return Optional.of(ERROR);
                }
            } else if (input instanceof Operation.SessionRequest || input instanceof FourLetterResponse) {
                // no-op                   
            } else {
                throw new IllegalArgumentException(input.toString());
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
            } else if (input instanceof Operation.SessionReply) {
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
                Operation.Response reply = ((Operation.SessionReply)input).reply();
                if (! (reply instanceof Operation.Error)) {
                    if (reply.opcode() == OpCode.CLOSE_SESSION) {
                        return Optional.of(DISCONNECTED);
                    }
                }
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
