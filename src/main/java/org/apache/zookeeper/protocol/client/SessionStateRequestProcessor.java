package org.apache.zookeeper.protocol.client;

import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.SessionConnectionState;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.util.Processor;

public class SessionStateRequestProcessor implements Processor<Operation.Request, Operation.Request>  {

    public static SessionStateRequestProcessor create(SessionConnectionState state) {
        return new SessionStateRequestProcessor(state);
    }

    protected final SessionConnectionState state;

    protected SessionStateRequestProcessor(SessionConnectionState state) {
        this.state = state;
    }
    
    public SessionConnectionState state() {
        return state;
    }

    @Override
    public Operation.Request apply(Operation.Request request) {
        switch (state.get()) {
        case CLOSING:
        case CLOSED:
        case ERROR:
            throw new IllegalStateException();
        default:
            break;
        }
        
        switch (request.operation()) {
        case CREATE_SESSION:
            state.set(SessionConnection.State.CONNECTING);
            break;
        case CLOSE_SESSION:
            state.set(SessionConnection.State.CLOSING);
            break;
        default:
            break;
        }
        
        return request;
    }
}
