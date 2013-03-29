package org.apache.zookeeper.protocol.server;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.SessionConnectionState;
import org.apache.zookeeper.protocol.OpCreateSessionAction;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.util.PipeProcessor;

import com.google.common.base.Optional;

public class SessionStateResponseProcessor implements PipeProcessor<Operation.Response>  {

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
    public Optional<Operation.Response> apply(Operation.Response response) {
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
            OpCreateSessionAction.Response createResponse = (OpCreateSessionAction.Response)response;
            if (createResponse.response().getSessionId() == Session.UNINITIALIZED_ID) {
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
        
        return Optional.of(response);
    }
}
