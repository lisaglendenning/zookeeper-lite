package org.apache.zookeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ConnectionEventValue;
import org.apache.zookeeper.ConnectionStateEvent;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.protocol.OpCreateSessionAction;
import org.apache.zookeeper.protocol.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

public class ConnectionManager {

    protected class ConnectionHandler implements FutureCallback<Operation.Result> {
        protected Connection connection;
        protected Session session;
        
        public ConnectionHandler(Connection connection) {
            this.connection = checkNotNull(connection);
            this.session = null;
            anonymousHandlers.add(this);
            connection.register(this);
        }
        
        public Session session() {
            return session;
        }

        public Connection connection() {
            return connection;
        }
        
        public void close() {
            if (session() != null) {
                synchronized (sessionHandlers) {
                    if (sessionHandlers.get(session().id()) == this) {
                        sessionHandlers.remove(session().id());
                        connection().unregister(this);
                    }
                }
            } else {
                if (anonymousHandlers.remove(this)) {
                    connection().unregister(this);
                }
            }
            connection().close();
        }
        
        @Subscribe
        public void handleEvent(ConnectionStateEvent event) {
            switch (event.event()) {
            case CLOSED:
                close();
                break;
            default:
                break;
            }
        }
        
        @Subscribe
        public void handleEvent(ConnectionEventValue<?> event) throws InterruptedException {
            assert (event.connection() == connection);
            Object value = event.event();
            if (value instanceof Operation.Request) {
                handleEvent((Operation.Request) value);
            } else if (value instanceof SessionConnection.State) {
                handleEvent((SessionConnection.State) value);
            }
        }

        @Subscribe
        public void handleEvent(SessionConnection.State event) {
            // TODO: right now, we don't close the connection
            // if the session expires and the connection doesn't close!
            switch (event) {
            case CLOSED:
                close();
                break;
            default:
                break;
            }
        }
        
        @Subscribe
        public void handleEvent(Operation.Request event) throws InterruptedException {
            logger.debug("Received request {} from {}", event, connection().remoteAddress());
            Session session = session();
            ListenableFuture<Operation.Result> future;
            if (session != null) {
                sessions().touch(session.id());
                future = executor().get(session.id()).submit(event);
            } else {
                if (event.operation() != Operation.CREATE_SESSION) {
                    // FIXME
                    throw new AssertionError();
                }
                future = executor().get().submit(event);
            }
            Futures.addCallback(future, this);
        }

        @Override
        public void onSuccess(Operation.Result result) {
            logger.debug("Sending result {} to {}", result, connection().remoteAddress());
            if (result.operation() == Operation.CREATE_SESSION) {
                long sessionId = ((OpCreateSessionAction.Response)result.response()).record().getSessionId();
                if (sessionId != Session.UNINITIALIZED_ID) {
                    this.session = sessions().get(sessionId);
                    assert session != null;
                    anonymousHandlers.remove(this);
                    synchronized (sessionHandlers) {
                        ConnectionHandler prev = sessionHandlers.remove(sessionId);
                        if (prev != null) {
                            prev.close();
                        }
                        sessionHandlers.put(sessionId, this);
                    }
                    logger.debug("Established session 0x{} with client {}", 
                            sessionId, connection().remoteAddress());
                } else {
                    logger.debug("Invalid session request {} from client {}",
                            result.request(), connection().remoteAddress());
                }
            }
            Connection connection = connection();
            switch (connection.state()) {
            case OPENING:
            case OPENED:
                connection.send(result);
                break;
            default:
                logger.debug("Dropping result: {}", result);
                break;
            }
        }

        @Override
        public void onFailure(Throwable t) {
            close();
        }
    }

    protected final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
    protected final ExpiringSessionManager sessions;
    protected final ServerConnectionGroup connections;
    protected final RequestExecutorService.Factory executor;
    protected final Set<ConnectionHandler> anonymousHandlers;
    protected final Map<Long, ConnectionHandler> sessionHandlers;
    
    @Inject
    public ConnectionManager(
            RequestExecutorService.Factory executor,
            ExpiringSessionManager sessions,
            ServerConnectionGroup connections) {
        this(executor, sessions, connections,
                Collections.synchronizedSet(Sets.<ConnectionHandler>newHashSet()),
                Collections.synchronizedMap(Maps.<Long, ConnectionHandler>newHashMap()));
    }
    
    protected ConnectionManager(
            RequestExecutorService.Factory executor,
            ExpiringSessionManager sessions,
            ServerConnectionGroup connections,
            Set<ConnectionHandler> anonymousHandlers,
            Map<Long, ConnectionHandler> sessionHandlers) {
        this.executor = executor;
        this.sessions = sessions;
        this.connections = connections;
        this.anonymousHandlers = anonymousHandlers;
        this.sessionHandlers = sessionHandlers;
        connections.register(this);
    }
    
    public ExpiringSessionManager sessions() {
        return sessions;
    }

    public RequestExecutorService.Factory executor() {
        return executor;
    }
    
    @Subscribe
    public void handleConnection(Connection connection) {
        newConnectionHandler(connection);
    }

    protected ConnectionHandler newConnectionHandler(Connection connection) {
        return new ConnectionHandler(connection);
    }
}
