package org.apache.zookeeper.protocol.client;

import java.io.IOException;
import java.io.InputStream;

import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.SessionConnectionState;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.data.Operations;
import com.google.common.base.Function;

public class SessionStateResponseDecoder {

    public static enum StateDecoder {

        CONNECTING {

            public SessionConnection.State state() {
                return SessionConnection.State.CONNECTING;
            }

            public Operation.Response decode(
                    Function<Integer, Operation> xidToOp, InputStream stream)
                    throws IOException {
                Operation op = Operation.CREATE_SESSION;
                Operation.Response response = Operations.Responses.decode(op,
                        stream);
                return response;
            }
        },

        CONNECTED {

            public SessionConnection.State state() {
                return SessionConnection.State.CONNECTED;
            }

            public Operation.Response decode(
                    Function<Integer, Operation> xidToOp, InputStream stream)
                    throws IOException {
                Operation.Response response = Operations.Responses.decode(
                        xidToOp, stream);
                return response;
            }
        };

        public static StateDecoder get(SessionConnection.State state) {
            switch (state) {
            case ANONYMOUS:
            case CONNECTING:
                return CONNECTING;
            case CONNECTED:
            case DISCONNECTING:
                return CONNECTED;
            default:
                throw new IllegalArgumentException();
            }
        }

        public abstract SessionConnection.State state();

        public abstract Operation.Response decode(
                Function<Integer, Operation> xidToOp, InputStream stream)
                throws IOException;
    }

    public static SessionStateResponseDecoder create(
            SessionConnectionState state) {
        return new SessionStateResponseDecoder(state);
    }

    protected final SessionConnectionState state;

    protected SessionStateResponseDecoder(SessionConnectionState state) {
        this.state = state;
    }

    public SessionConnectionState state() {
        return state;
    }

    public Operation.Response decode(Function<Integer, Operation> xidToOp,
            InputStream stream) throws IOException {
        StateDecoder decoder = StateDecoder.get(state.get());
        Operation.Response response = decoder.decode(xidToOp, stream);

        switch (response.operation()) {
        case CREATE_SESSION:
            // Note that if the returned timeOut == 0
            // (and other fields will be set to zero) then
            // this means "invalid request" and the server will now
            // close the connection without sending anything else
            if (response instanceof Operation.Error) {
                state.set(SessionConnection.State.ERROR);
            } else {
                state.set(SessionConnection.State.CONNECTED);
            }
            break;
        case CLOSE_SESSION:
            state.set(SessionConnection.State.DISCONNECTED);
            break;
        default:
            break;
        }

        return response;
    }
}
