package org.apache.zookeeper.protocol.server;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.SessionConnectionState;
import org.apache.zookeeper.protocol.OpCreateSessionAction;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.util.Processor;

public class SessionStateResponseProcessor implements Processor<Operation.Response, Operation.Response>  {

    public static SessionStateResponseProcessor create(SessionConnectionState state) {
        return new SessionStateResponseProcessor(state);
    }

    protected final SessionConnectionState state;

    protected SessionStateResponseProcessor(SessionConnectionState state) {
        this.state = state;
    }
    
    public SessionConnectionState state() {
        return state;
    }

    @Override
    public Operation.Response apply(Operation.Response response) {
        switch (state.get()) {
        case CLOSED:
        case ERROR:
            throw new IllegalStateException();
        default:
            break;
        }
        
        switch (response.operation()) {
        case CREATE_SESSION:
        {
            OpCreateSessionAction.Response createResponse;
            if (response instanceof Operation.Result) {
                createResponse = (OpCreateSessionAction.Response) ((Operation.Result)response).response();
            } else {
                createResponse = (OpCreateSessionAction.Response)response;
            }
            if (createResponse.record().getSessionId() == Session.UNINITIALIZED_ID) {
                state.set(SessionConnection.State.ERROR);
            } else {
                state.set(SessionConnection.State.CONNECTED);
            }
            break;
        }
        case CLOSE_SESSION:
            state.set(SessionConnection.State.CLOSED);
            break;
        default:
            break;
        }
        
        return response;
    }
}
