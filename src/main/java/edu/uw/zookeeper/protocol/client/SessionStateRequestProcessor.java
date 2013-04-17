package edu.uw.zookeeper.protocol.client;

import edu.uw.zookeeper.SessionConnection;
import edu.uw.zookeeper.SessionConnectionState;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.util.Processor;

public class SessionStateRequestProcessor implements
        Processor<Operation.Request, Operation.Request> {

    public static SessionStateRequestProcessor create(
            SessionConnectionState state) {
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
        case DISCONNECTING:
        case DISCONNECTED:
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
            state.set(SessionConnection.State.DISCONNECTING);
            break;
        default:
            break;
        }

        return request;
    }
}