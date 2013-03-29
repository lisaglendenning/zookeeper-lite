package org.apache.zookeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ConnectionEventValue;
import org.apache.zookeeper.ConnectionStateEvent;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionEventValue;
import org.apache.zookeeper.SessionStateEvent;
import org.apache.zookeeper.protocol.OpCallRequest;
import org.apache.zookeeper.protocol.OpCreateSessionAction;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Operation.Request;
import org.apache.zookeeper.protocol.Operations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

public class SessionConnectionManager {

    protected class ReplayEvent implements FutureCallback<Void> {
        protected ConnectionEventValue<Request> event;
        
        public ReplayEvent(ConnectionEventValue<Request> event) {
            this.event = event;
        }
        
        @Override
        public void onFailure(Throwable cause) {
            // TODO
            throw new AssertionError(cause);
        }
    
        @Override
        public void onSuccess(Void result) {
            handleEvent(event);
        }
    }

    protected final Logger logger = LoggerFactory.getLogger(SessionConnectionManager.class);

    protected final RequestManager processor;
    protected final ExpiringSessionManager sessions;
    protected final ServerConnectionGroup connections;
    protected final BiMap<Long, Connection> sessionConnections;
    
    @Inject
    public SessionConnectionManager(
            RequestManager requests,
            ExpiringSessionManager sessions,
            ServerConnectionGroup connections) {
        this(requests, sessions, connections,
                Maps.synchronizedBiMap(HashBiMap.<Long, Connection>create()));
    }
    
    protected SessionConnectionManager(
            RequestManager requests,
            ExpiringSessionManager sessions,
            ServerConnectionGroup connections,
            BiMap<Long, Connection> sessionConnections) {
        this.processor = requests;
        this.sessions = sessions;
        this.connections = connections;
        this.sessionConnections = sessionConnections;
        connections.register(this);
        sessions.register(this);
    }
    
    protected BiMap<Long, Connection> connections() {
        return sessionConnections;
    }
    
    public ExpiringSessionManager sessions() {
        return sessions;
    }

    public RequestManager processor() {
        return processor;
    }
    
    public synchronized Connection connection(long sessionId) {
        return connections().get(sessionId);
    }

    public synchronized Connection connection(Session session) {
        return connections().get(checkNotNull(session).id());
    }
    
    public synchronized Session session(Connection connection) {
        Session session = null;
        Map<Connection, Long> inverse = connections().inverse();
        if (inverse.containsKey(connection)) {
            long id = inverse.get(checkNotNull(connection));
            session = sessions().get(id);
        }
        return session;
    }
    
    @Subscribe
    public void handleConnection(Connection connection) {
        connection.register(this);
    }
    
    @SuppressWarnings("unchecked")
    @Subscribe
    public void handleEvent(ConnectionEventValue<?> event) {
        Object value = event.event();
        if (value instanceof Operation.Request) {
            handleOperationRequest((ConnectionEventValue<Operation.Request>) event);
        }
    }

    @Subscribe
    public void handleEvent(SessionStateEvent event) {
        Session session = checkNotNull(event.session());
        Connection connection = connection(session.id());
        switch (event.event()) {
        case CLOSED:
        {
            // we assume that if a session has been closed
            // then the OpCloseSessionResponse has already been sent
            connection = connections().remove(session.id());
            if (connection != null) {
                switch (connection.state()) {
                case CLOSING:
                case CLOSED:
                    break;
                default:
                    connection.close();
                    break;
                }
            }
            break;
        }
        case EXPIRED:
        {
            // we are the ones who initiate the close request
            Operation.CallRequest request = OpCallRequest.create(0,
                    Operations.Requests.create(Operation.CLOSE_SESSION));
            Object response = processor().submit(new SessionEventValue(session, request));
            if (connection != null) {
                connection.send(response);
            }
            break;
        }
        default:
            break;
        }
    }

    @Subscribe
    public void handleEvent(ConnectionStateEvent event) {
        Connection connection = event.connection();
        switch (event.event()) {
        case CLOSED:
            sessionConnections.inverse().remove(connection);
            break;
        default:
            break;
        }
    }
    
    public void handleOperationRequest(
            ConnectionEventValue<Operation.Request> event) {
        Operation.Request request = event.event();
        Connection connection = event.connection();
        Session session = session(connection);
        if (session != null) {
            sessions().touch(session.id());
            Object response = processor().submit(new SessionEventValue(session, request));
            if (connection != null) {
                connection.send(response);
            }
        } else {
            if (request.operation() != Operation.CREATE_SESSION) {
                // FIXME
                throw new AssertionError();
            }
            OpCreateSessionAction.Request createRequest = (OpCreateSessionAction.Request)request;
            synchronized (this) {
                // First, check if we already have a connection for this session
                // and if we do, close it and try again
                long id = createRequest.request().getSessionId();
                if (id != Session.UNINITIALIZED_ID) {
                    Connection prevConnection = connection(id);
                    if (prevConnection != null && connection != null && prevConnection != connection) {
                        ListenableFuture<Void> future = prevConnection.close();
                        // FIXME: make sure this triggers removal before the callback is called
                        Futures.addCallback(future, new ReplayEvent(event));
                        return;
                    }
                }

                OpCreateSessionAction.Response response = (OpCreateSessionAction.Response) processor().submit(request);
                id = response.response().getSessionId();
                if (id != Session.UNINITIALIZED_ID) {
                    if (connection != null) {
                        assert(connections().get(id) == null);
                        connections().put(id, connection);
                        logger.info("Established session 0x{} with client {}", 
                                id, connection.remoteAddress());
                    }
                } else {
                    if (connection != null) {
                        logger.info("Invalid session request {} from client {}",
                                request, connection.remoteAddress());
                    }
                }
                if (connection != null) {
                    connection.send(response);
                }
            }
        }
    }
}
