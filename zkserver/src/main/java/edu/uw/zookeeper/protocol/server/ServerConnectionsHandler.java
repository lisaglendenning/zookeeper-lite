package edu.uw.zookeeper.protocol.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

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
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.TelnetCloseRequest;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.server.ServerConnectionFactoryBuilder;
import edu.uw.zookeeper.server.SessionExecutor;
import edu.uw.zookeeper.server.SessionStateEvent;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.References;

public class ServerConnectionsHandler<C extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>> extends AbstractIdleService {
    
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
        protected final ServerExecutor serverExecutor;
        
        public Builder(
                ServerConnectionFactoryBuilder connectionBuilder,
                TimeValue timeOut,
                ServerExecutor serverExecutor) {
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

        public ServerExecutor getServerExecutor() {
            return serverExecutor;
        }

        public Builder setServerExecutor(ServerExecutor serverExecutor) {
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
                ServerExecutor serverExecutor) {
            return new Builder(connectionBuilder, timeOut, serverExecutor);
        }

        protected List<Service> doBuild() {
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connections = connectionBuilder.build();
            ServerConnectionsHandler<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> handler = ServerConnectionsHandler.newInstance(
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
    
    public static <C extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>> ServerConnectionsHandler<C> newInstance(
            ServerExecutor server, 
            ScheduledExecutorService scheduler, 
            TimeValue timeOut) {
        return new ServerConnectionsHandler<C>(server, scheduler, timeOut, new MapMaker());
    }
    
    protected final TimeValue timeOut;
    protected final ScheduledExecutorService scheduler;
    protected final ServerExecutor server;
    protected final ConcurrentMap<C, ConnectionHandler<?,?>> handlers;
    
    protected ServerConnectionsHandler(
            ServerExecutor server, 
            ScheduledExecutorService scheduler, 
            TimeValue timeOut,
            MapMaker maker) {
        this.server = server;
        this.scheduler = scheduler;
        this.timeOut = timeOut;
        this.handlers = maker.weakKeys().weakValues().makeMap();
    }
    
    @Handler
    public void handleNewConnection(C connection) {
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
        for (ConnectionHandler<?,?> handler: handlers.values()) {
            handler.stop();
        }
    }

    @net.engio.mbassy.listener.Listener(references = References.Strong)
    protected abstract class ConnectionHandler<I extends Message.Client, O extends Message.Server> extends AbstractActor<I> implements FutureCallback<O> {

        protected final Logger logger;
        protected final C connection;
        
        protected ConnectionHandler(C connection) {
            this.logger = LogManager.getLogger(getClass());
            this.connection = connection;
            
            handlers.put(connection, this);
            connection.subscribe(this);
            
            // because we are using strong references we need to be careful
            // to always unsubscribe when the channel closes
            if (connection.state().compareTo(Connection.State.CONNECTION_CLOSING) >= 0) {
                stop();
            }
        }

        @Handler
        public void handleTransitionEvent(Automaton.Transition<?> event) {
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

        @Override
        protected Logger logger() {
            return logger;
        }
    }
    
    @net.engio.mbassy.listener.Listener(references = References.Strong)
    protected class AnonymousConnectionHandler extends ConnectionHandler<Message.Client, Message.Server> {
        
        protected final TimeOutCallback timer;
        
        public AnonymousConnectionHandler(C connection) {
            super(connection);
            this.timer = TimeOutCallback.create(
                    TimeOutParameters.create(timeOut), scheduler, this);
        }
        
        @Handler
        public void handleMessage(Message.Client message) {
            send(message);
        }
        
        @Override
        public synchronized void onSuccess(Message.Server result) {
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

        @Override
        protected boolean doSend(Message.Client message) {
            timer.send(message);
            if (message instanceof FourLetterRequest) {
                Futures.addCallback(server.anonymousExecutor().submit((FourLetterRequest) message), this, connection);
            } else if (message instanceof TelnetCloseRequest) {
                connection.close();
            } else if (message instanceof ConnectMessage.Request) {
                // now we know that no more messages will be read 
                // by the connection until we write the response
                Futures.addCallback(server.connectExecutor().submit(Pair.create((ConnectMessage.Request) message, connection)), this, connection);
            } else {
                throw new AssertionError(String.valueOf(message));
            }
            return true;
        }

        @Override
        protected void doStop() {
            timer.stop();
            super.doStop();
        }
    }

    @net.engio.mbassy.listener.Listener(references = References.Strong)
    protected class SessionConnectionHandler extends ConnectionHandler<Message.ClientRequest<?>, Message.ServerResponse<?>> {
        
        protected final SessionExecutor session;
        
        public SessionConnectionHandler(
                SessionExecutor session, C connection) {
            super(connection);
            this.session = session;
            
            if (state() != State.TERMINATED) {
                session.subscribe(this);
            }
        }

        @Handler
        public void handleMessage(Message.ClientRequest<?> message) {
            send(message);
        }
        
        @Handler
        public void handleSessionEvent(SessionStateEvent event) {
            if (event.event() == Session.State.SESSION_EXPIRED) {
                onFailure(new KeeperException.SessionExpiredException());
            }
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
        public void onSuccess(Message.ServerResponse<?> result) {
            logger.debug("Sending {} ({})", result, this);
            connection.write(result);
        }
        
        @Override
        protected boolean doSend(Message.ClientRequest<?> message) {
            Futures.addCallback(session.submit(message), this, connection);
            return true;
        }

        @Override
        protected void doStop() {
            session.unsubscribe(this);
            super.doStop();
        }
    }
}
