package org.apache.zookeeper.protocol.server;

import java.io.IOException;
import java.io.InputStream;

import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.SessionConnectionState;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.data.Operations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class SessionStateRequestDecoder {

    public static enum StateDecoder {

        ANONYMOUS {

            public SessionConnection.State state() { return SessionConnection.State.ANONYMOUS; }
            
            public Operation.Request decode(InputStream stream) throws IOException {
                Operation op = Operation.CREATE_SESSION;
                Operation.Request request = Operations.Requests.decode(op, stream);
                return request;
            }
        },
        
        CONNECTED {

            public SessionConnection.State state() { return SessionConnection.State.CONNECTED; }
            
            public Operation.Request decode(InputStream stream) throws IOException {
                Operation.Request request = Operations.Requests.decode(stream);
                return request;
            }
        };

        public abstract SessionConnection.State state();
        
        public abstract Operation.Request decode(InputStream stream) throws IOException;
    }
    
    protected final Logger logger = LoggerFactory.getLogger(SessionStateRequestDecoder.class);

    protected SessionConnectionState state;

    public static SessionStateRequestDecoder create(SessionConnectionState state) {
        return new SessionStateRequestDecoder(state);
    }
    
    @Inject
    protected SessionStateRequestDecoder(SessionConnectionState state) {
        super();
        this.state = state;
    }

    public SessionConnectionState state() {
        return state;
    }

    public Operation.Request decode(InputStream stream) throws IOException {
        StateDecoder decoder = null;
        switch (state.get()) {
        case ANONYMOUS:
            decoder = StateDecoder.ANONYMOUS;
            break;
        case CONNECTING:
        case CONNECTED:
            decoder = StateDecoder.CONNECTED;
            break;
        default:
            throw new IllegalStateException();
        }

        Operation.Request request = decoder.decode(stream);
        if (logger.isTraceEnabled()) {
            logger.trace("Decoded: {}", request);
        }
        
        switch (request.operation()) {
        case CREATE_SESSION:
            state.set(SessionConnection.State.CONNECTING);
            break;
        case CLOSE_SESSION:
            state.set(SessionConnection.State.DISCONNECTING);
            break;
        default:
            break;
        }
        
        return request;
    }
}
