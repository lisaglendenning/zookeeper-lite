package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.SessionConnection;
import edu.uw.zookeeper.SessionConnectionState;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.util.Processor;

public class SessionStateResponseProcessor implements
        Processor<Operation.Response, Operation.Response> {

    public static SessionStateResponseProcessor create(
            SessionConnectionState state) {
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
        case DISCONNECTED:
        case ERROR:
            throw new IllegalStateException();
        default:
            break;
        }

        switch (response.operation()) {
        case CREATE_SESSION: {
            if (response instanceof Operation.Error) {
                state.set(SessionConnection.State.ERROR);
            } else {
                state.set(SessionConnection.State.CONNECTED);
            }
            break;
        }
        case CLOSE_SESSION:
            state.set(SessionConnection.State.DISCONNECTED);
            break;
        default:
            break;
        }

        return response;
    }
}
