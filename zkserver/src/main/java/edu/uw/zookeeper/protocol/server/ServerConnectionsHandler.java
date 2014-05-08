package edu.uw.zookeeper.protocol.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.TelnetCloseRequest;
import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import net.engio.mbassy.listener.Handler;

public class ServerConnectionsHandler<C extends ServerProtocolConnection<?,?>> extends AbstractIdleService implements ConnectionFactory.ConnectionsListener<C> {
    
    public static Builder builder() {
        return Builder.defaults();
    }
    
    public static class Builder implements ZooKeeperApplication.RuntimeBuilder<List<Service>, Builder> {

        public static Builder defaults() {
            return new Builder(ServerConnectionFactoryBuilder.defaults(), 
                    null, null);
        }
        
        protected final ServerConnectionFactoryBuilder connectionBuilder;
        protected final TimeValue timeOut;
        protected final ServerExecutor<?> serverExecutor;
        
        public Builder(
                ServerConnectionFactoryBuilder connectionBuilder,
                TimeValue timeOut,
                ServerExecutor<?> serverExecutor) {
            this.timeOut = timeOut;
            this.connectionBuilder = checkNotNull(connectionBuilder);
            this.serverExecutor = serverExecutor;
        }

        @Override
        public RuntimeModule getRuntimeModule() {
            return connectionBuilder.getRuntimeModule();
        }

        @Override
        public Builder setRuntimeModule(RuntimeModule runtime) {
            return setConnectionBuilder(connectionBuilder.setRuntimeModule(runtime));
        }

        public ServerConnectionFactoryBuilder getConnectionBuilder() {
            return connectionBuilder;
        }

        public Builder setConnectionBuilder(ServerConnectionFactoryBuilder connectionBuilder) {
            if (this.connectionBuilder == connectionBuilder) {
                return this;
            } else {
                return newInstance(connectionBuilder, timeOut, serverExecutor);
            }
        }
        
        public TimeValue getTimeOut() {
            return timeOut;
        }

        public Builder setTimeOut(TimeValue timeOut) {
            if (this.timeOut == timeOut) {
                return this;
            } else {
                return newInstance(connectionBuilder, timeOut, serverExecutor);
            }
        }

        public ServerExecutor<?> getServerExecutor() {
            return serverExecutor;
        }

        public Builder setServerExecutor(ServerExecutor<?> serverExecutor) {
            if (this.serverExecutor == serverExecutor) {
                return this;
            } else {
                return newInstance(connectionBuilder, timeOut, serverExecutor);
            }
        }

        @Override
        public Builder setDefaults() {
            checkNotNull(getServerExecutor());
            
            if (getTimeOut() == null) {
                return setTimeOut(getDefaultTimeOut()).setDefaults();
            }
            ServerConnectionFactoryBuilder connectionBuilder = getDefaultConnectionBuilder();
            if (this.connectionBuilder != connectionBuilder) {
                return setConnectionBuilder(connectionBuilder).setDefaults();
            }
            return this;
        }
        
        @Override
        public List<Service> build() {
            return setDefaults().doBuild();
        }

        protected Builder newInstance(
                ServerConnectionFactoryBuilder connectionBuilder,
                TimeValue timeOut,
                ServerExecutor<?> serverExecutor) {
            return new Builder(connectionBuilder, timeOut, serverExecutor);
        }

        protected List<Service> doBuild() {
            ServerConnectionFactory<? extends ServerProtocolConnection<?,?>> connections = connectionBuilder.build();
            ServerConnectionsHandler<ServerProtocolConnection<?,?>> handler = ServerConnectionsHandler.newInstance(
                    getServerExecutor(),
                    getRuntimeModule().getExecutors().get(ScheduledExecutorService.class),
                    getTimeOut());
            connections.subscribe(handler);
            return Lists.<Service>newArrayList(
                    connections,
                    handler);
        }
        
        protected TimeValue getDefaultTimeOut() {
            return ZooKeeperApplication.ConfigurableTimeout.get(
                    getRuntimeModule().getConfiguration());
        }
        
        protected ServerConnectionFactoryBuilder getDefaultConnectionBuilder() {
            return getConnectionBuilder().setDefaults();
        }
    }
    
    public static <C extends ServerProtocolConnection<?,?>> ServerConnectionsHandler<C> newInstance(
            ServerExecutor<?> server, 
            ScheduledExecutorService scheduler, 
            TimeValue timeOut) {
        // Assume Connection stores weak references to listeners
        // so store strong references to handlers in map
        ConcurrentMap<C, ServerConnectionsHandler<C>.ConnectionHandler<?>> handlers = new MapMaker().makeMap();
        return new ServerConnectionsHandler<C>(server, scheduler, timeOut, handlers);
    }

    protected final Logger logger;
    protected final TimeValue timeOut;
    protected final ScheduledExecutorService scheduler;
    protected final ServerExecutor<?> server;
    protected final ConcurrentMap<C, ConnectionHandler<?>> handlers;
    
    protected ServerConnectionsHandler(
            ServerExecutor<?> server, 
            ScheduledExecutorService scheduler, 
            TimeValue timeOut,
            ConcurrentMap<C, ConnectionHandler<?>> handlers) {
        this.logger = LogManager.getLogger(getClass());
        this.server = server;
        this.scheduler = scheduler;
        this.timeOut = timeOut;
        this.handlers = handlers;
    }
    
    @Override
    public void handleConnectionOpen(C connection) {
        logger.debug("New connection {}", connection);
        AnonymousConnectionHandler handler = new AnonymousConnectionHandler(connection);
        switch (state()) {
        case FAILED:
        case STOPPING:
        case TERMINATED:
            handler.stop();
            break;
        default:
            break;
        }
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        for (ConnectionHandler<?> handler: handlers.values()) {
            handler.stop();
        }
    }

    protected abstract class ConnectionHandler<I extends Message.Client> extends AbstractActor<I> implements Connection.Listener<Message.Client>, FutureCallback<Object> {

        protected final C connection;
        
        protected ConnectionHandler(C connection, Logger logger) {
            super(logger);
            this.connection = connection;
            
            handlers.put(connection, this);
            connection.subscribe(this);
            
            if (connection.state() == Connection.State.CONNECTION_CLOSED) {
                stop();
            }
        }

        @Override
        public void handleConnectionState(Automaton.Transition<Connection.State> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                stop();
            }
        }

        @Override
        public void onFailure(Throwable t) {
            logger.debug("{}", this, t);
            connection.close();
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("connection", connection)
                    .toString();
        }
        
        @Override
        protected void doRun() throws Exception {
        }

        @Override
        protected void doStop() {
            connection.unsubscribe(this);
            handlers.remove(connection, this);
        }
    }
    
    protected class AnonymousConnectionHandler extends ConnectionHandler<Message.Client> {
        
        protected final TimeOutActor<Message.Client, Void> timer;
        
        public AnonymousConnectionHandler(C connection) {
            super(connection, LogManager.getLogger(AnonymousConnectionHandler.class));
            this.timer = TimeOutActor.create(
                    TimeOutParameters.milliseconds(timeOut.value(TimeUnit.MILLISECONDS)), scheduler,
                    LogManager.getLogger(AnonymousConnectionHandler.class));
            new TimeOutListener();
            timer.run();
        }

        @Override
        public void handleConnectionRead(Message.Client message) {
            send(message);
        }
        
        @Override
        public synchronized void onSuccess(Object result) {
            if ((result != null) && (result instanceof Message.Server)) {
                logger.debug("Sending {} ({})", result, this);
                if (result instanceof FourLetterResponse) {
                    // it's possible that this response happens after
                    // the session connects
                    // in which case, we need to just drop it
                    if (handlers.get(connection) == this) {
                        connection.write((FourLetterResponse) result);
                    }
                } else if (result instanceof ConnectMessage.Response) {
                    ConnectMessage.Response response = (ConnectMessage.Response) result;
                    if (response instanceof ConnectMessage.Response.Valid) {
                        SessionExecutor session = server.sessionExecutor(response.getSessionId());
                        // must subscribe new handler 
                        // before sending the response
                        new SessionConnectionHandler(session, connection);
                    } else {
                        // if the response is Invalid, we want the response
                        // to be flushed to the client before closing the connection
                        // which ProtocolCodecConnection will do for us
                    }
                    
                    // must unsubscribe before sending response
                    stop();
                    
                    // this write will trigger reading messages again
                    connection.write(response);
                } else {
                    throw new AssertionError(String.valueOf(result));
                }
            }
        }

        @Override
        protected boolean doSend(Message.Client message) {
            timer.send(message);
            if (message instanceof FourLetterRequest) {
                Futures.addCallback(server.anonymousExecutor().submit((FourLetterRequest) message), this, SameThreadExecutor.getInstance());
            } else if (message instanceof TelnetCloseRequest) {
                connection.close();
            } else if (message instanceof ConnectMessage.Request) {
                // now we know that no more messages will be read 
                // by the connection until we write the response
                Futures.addCallback(server.connectExecutor().submit((ConnectMessage.Request) message), this, SameThreadExecutor.getInstance());
            } else {
                throw new AssertionError(String.valueOf(message));
            }
            return true;
        }

        @Override
        protected void doStop() {
            timer.cancel(false);
            super.doStop();
        }
        
        protected class TimeOutListener implements Runnable {

            public TimeOutListener() {
                timer.addListener(this, SameThreadExecutor.getInstance());
            }
            
            @Override
            public void run() {
                if (timer.isDone()) {
                    if (!timer.isCancelled()) {
                        try {
                            timer.get();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        } catch (ExecutionException e) {
                            onFailure(e.getCause());
                        }
                    }
                }
            }
        }
    }

    protected class SessionConnectionHandler extends ConnectionHandler<Message.ClientRequest<?>> implements SessionListener {
        
        protected final SessionExecutor session;
        
        public SessionConnectionHandler(
                SessionExecutor session, C connection) {
            super(connection, LogManager.getLogger(SessionConnectionHandler.class));
            this.session = session;
            
            session.subscribe(this);
        }

        @Override
        public void handleConnectionRead(Message.Client message) {
            send((Message.ClientRequest<?>) message);
        }

        @Override
        public void handleAutomatonTransition(Automaton.Transition<ProtocolState> event) {
            if (event.to() == ProtocolState.ERROR) {
                onFailure(new KeeperException.SessionExpiredException());
            }
        }

        @Override
        public void handleNotification(
                Operation.ProtocolResponse<IWatcherEvent> notification) {
            onSuccess(notification);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("session", Session.toString(session.session().id()))
                    .add("connection", connection)
                    .toString();
        }
        
        @Handler
        @Override
        public void onSuccess(Object result) {
            if ((result != null) && (result instanceof Message.Server)) {
                logger.debug("Sending {} ({})", result, this);
                connection.write((Message.Server) result);
            }
        }
        
        @Override
        protected boolean doSend(Message.ClientRequest<?> message) {
            Futures.addCallback(session.submit(message), this, SameThreadExecutor.getInstance());
            return true;
        }

        @Override
        protected void doStop() {
            session.unsubscribe(this);
            super.doStop();
        }
    }
}
