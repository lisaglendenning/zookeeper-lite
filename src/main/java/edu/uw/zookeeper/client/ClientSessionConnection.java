package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.ConnectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.typesafe.config.ConfigException;

import edu.uw.zookeeper.Connection;
import edu.uw.zookeeper.RequestExecutorService;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.SessionConnection;
import edu.uw.zookeeper.SessionConnectionState;
import edu.uw.zookeeper.SessionParameters;
import edu.uw.zookeeper.Zxid;
import edu.uw.zookeeper.data.OpCallResult;
import edu.uw.zookeeper.data.OpCreateSessionAction;
import edu.uw.zookeeper.data.OpResult;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.event.ConnectionMessageEvent;
import edu.uw.zookeeper.event.SessionConnectionStateEvent;
import edu.uw.zookeeper.event.SessionResponseEvent;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.util.Configurable;
import edu.uw.zookeeper.util.ConfigurableTime;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.SettableTask;

public class ClientSessionConnection extends ForwardingEventful implements
        RequestExecutorService, SessionConnection {

    public static class Factory implements Configurable {

        public static Factory create(Configuration configuration,
                Provider<Eventful> eventfulFactory) {
            return new Factory(configuration, eventfulFactory);
        }

        public static final String CONFIG_PATH = "Client.Timeout";
        public static final long DEFAULT_TIMEOUT_VALUE = 30;
        public static final String DEFAULT_TIMEOUT_UNIT = "SECONDS";

        protected final ConfigurableTime timeOut;
        protected Provider<Eventful> eventfulFactory;

        @Inject
        protected Factory(Configuration configuration,
                Provider<Eventful> eventfulFactory) {
            this(eventfulFactory);
            configure(configuration);
        }

        protected Factory(
                Provider<Eventful> eventfulFactory) {
            this.eventfulFactory = eventfulFactory;
            this.timeOut = ConfigurableTime.create(
                    DEFAULT_TIMEOUT_VALUE,
                    DEFAULT_TIMEOUT_UNIT);
        }

        public ClientSessionConnection get(Connection connection) {
            return ClientSessionConnection.create(connection, eventfulFactory,
                    timeOut);
        }

        @Override
        public void configure(Configuration configuration) {
            try {
                this.timeOut.get(configuration.get().getConfig(CONFIG_PATH));
            } catch (ConfigException.Missing e) {}
        }
    }

    public static class ConnectionFactory extends Factory implements
            Provider<ClientSessionConnection> {

        public static ConnectionFactory create(Configuration configuration,
                Provider<Eventful> eventfulFactory,
                Provider<Connection> connectionFactory) {
            return new ConnectionFactory(configuration, eventfulFactory,
                    connectionFactory);
        }

        protected final Provider<Connection> connectionFactory;

        @Inject
        protected ConnectionFactory(Configuration configuration,
                Provider<Eventful> eventfulFactory,
                Provider<Connection> connectionFactory) {
            super(configuration, eventfulFactory);
            this.connectionFactory = connectionFactory;
        }

        @Override
        public ClientSessionConnection get() {
            return get(connectionFactory.get());
        }
    }

    public static ClientSessionConnection create(Connection connection,
            Provider<Eventful> eventfulFactory, ConfigurableTime timeOut) {
        return new ClientSessionConnection(connection, eventfulFactory, timeOut);
    }

    public static ClientSessionConnection create(Connection connection,
            Provider<Eventful> eventfulFactory, ConfigurableTime timeOut,
            Zxid zxid) {
        return new ClientSessionConnection(connection, eventfulFactory,
                timeOut, zxid);
    }

    public static ClientSessionConnection create(Connection connection,
            Provider<Eventful> eventfulFactory, ConfigurableTime timeOut,
            Zxid zxid, Session session) {
        return new ClientSessionConnection(connection, eventfulFactory,
                timeOut, zxid, session);
    }

    protected final Logger logger = LoggerFactory
            .getLogger(ClientSessionConnection.class);
    protected Session session;
    protected final Connection connection;
    protected final SessionConnectionState state;
    protected final ConfigurableTime timeOut;
    protected final Zxid zxid;
    protected final BlockingQueue<SettableTask<Operation.Request, Operation.Result>> tasks;

    @Inject
    protected ClientSessionConnection(Connection connection,
            Provider<Eventful> eventfulFactory, ConfigurableTime timeOut) {
        this(connection, eventfulFactory, timeOut, Zxid.create());
    }

    protected ClientSessionConnection(Connection connection,
            Provider<Eventful> eventfulFactory, ConfigurableTime timeOut,
            Zxid zxid) {
        this(connection, eventfulFactory, timeOut, zxid, Session.create());
    }

    protected ClientSessionConnection(Connection connection,
            Provider<Eventful> eventfulFactory, ConfigurableTime timeOut,
            Zxid zxid, Session session) {
        super(eventfulFactory.get());
        this.session = checkNotNull(session);
        this.timeOut = checkNotNull(timeOut);
        this.zxid = checkNotNull(zxid);
        this.connection = checkNotNull(connection);
        this.state = SessionConnectionState.create(eventfulFactory.get());
        this.tasks = new LinkedBlockingQueue<SettableTask<Operation.Request, Operation.Result>>();
    }

    public Connection connection() {
        return connection;
    }

    public Session session() {
        return session;
    }

    @Override
    public SessionConnection.State state() {
        return state.get();
    }

    public ListenableFuture<Operation.Result> connect() {
        boolean valid = state.compareAndSet(State.ANONYMOUS, State.CONNECTING);
        if (!valid) {
            SettableFuture<Operation.Result> future = SettableFuture.create();
            switch (state()) {
            case CONNECTING:
            case CONNECTED:
                future.set(null);
                break;
            default:
                future.setException(new IllegalStateException());
            }
            return future;
        }

        state.register(this);
        connection.register(this);

        OpCreateSessionAction.Request message = Operations.Requests
                .create(Operation.CREATE_SESSION);
        ConnectRequest request = message.record();
        request.setProtocolVersion(Records.PROTOCOL_VERSION);
        request.setTimeOut(timeOut.get().value(TimeUnit.MILLISECONDS).intValue());
        request.setLastZxidSeen(zxid.get());
        if (session().initialized()) {
            request.setSessionId(session().id());
            request.setPasswd(session().parameters().password());
        } else {
            request.setSessionId(Session.UNINITIALIZED_ID);
            request.setPasswd(SessionParameters.NO_PASSWORD);
        }

        return send(message);
    }

    public ListenableFuture<Operation.Result> disconnect() {
        boolean valid = state.compareAndSet(State.CONNECTED,
                State.DISCONNECTING);
        if (!valid) {
            SettableFuture<Operation.Result> future = SettableFuture.create();
            switch (state()) {
            case DISCONNECTING:
            case DISCONNECTED:
                future.set(null);
                break;
            default:
                future.setException(new IllegalStateException());
            }
            return future;
        }

        Operation.Request message = Operations.Requests
                .create(Operation.CLOSE_SESSION);
        return send(message);
    }

    @Override
    public ListenableFuture<Operation.Result> submit(Operation.Request request) {
        SessionConnection.State sessionState = state();
        switch (sessionState) {
        case CONNECTING:
        case CONNECTED:
            break;
        default:
            throw new IllegalStateException(sessionState.toString());
        }

        switch (request.operation()) {
        case CREATE_SESSION:
        case CLOSE_SESSION:
            throw new IllegalArgumentException(request.operation().toString());
        default:
            break;
        }

        return send(request);
    }

    protected ListenableFuture<Operation.Result> send(Operation.Request request) {
        Connection.State connectionState = connection().state();
        switch (connectionState) {
        case CONNECTION_OPENING:
        case CONNECTION_OPENED:
            break;
        default:
            throw new IllegalStateException();
        }

        SettableTask<Operation.Request, Operation.Result> task = SettableTask
                .create(request);
        request = task.task();
        // ensure that tasks are sent in the same as the order of this queue...
        synchronized (this) {
            logger.debug("Sending {}", request);
            tasks.add(task);
            try {
                connection.send(request);
            } catch (Exception e) {
                task.future().setException(e);
            }
        }
        return task.future();
    }

    @Subscribe
    public void handleEvent(SessionConnection.State event) {
        post(SessionConnectionStateEvent.create(session(), event));
    }

    @Subscribe
    public void handleEvent(ConnectionMessageEvent<?> event) {
        Object value = event.event();
        // TODO: do we care about other events?
        if (value instanceof Operation.Response) {
            handleEvent((Operation.Response) value);
        }
    }

    @Subscribe
    public void handleEvent(Operation.Response event) {
        SessionConnection.State sessionState = state();
        switch (sessionState) {
        case CONNECTING:
        case CONNECTED:
        case DISCONNECTING:
            break;
        default:
            // don't care?
            return;
        }

        Operation.Response response = event;
        Operation.Result result = null;
        if (event instanceof Operation.Result) {
            result = (Operation.Result) event;
            response = result.response();
        }

        switch (event.operation()) {
        case CREATE_SESSION: {
            if (!(response instanceof Operation.Error)) {
                OpCreateSessionAction.Response opResponse = (OpCreateSessionAction.Response) response;
                this.session = Session.create(opResponse);
                this.state.set(SessionConnection.State.CONNECTED);
            }
            break;
        }
        case CLOSE_SESSION: {
            if (!(response instanceof Operation.Error)) {
                this.state.set(SessionConnection.State.DISCONNECTED);
            }
            break;
        }
        default:
            break;
        }

        if (event instanceof Operation.CallResponse) {
            synchronized (zxid) {
                long eventZxid = ((Operation.CallResponse) event).zxid();
                long lastZxid = zxid.get();
                if (eventZxid > lastZxid) {
                    zxid.compareAndSet(lastZxid, eventZxid);
                }
            }
        }

        SettableTask<Operation.Request, Operation.Result> task = null;
        synchronized (this) {
            task = tasks.peek();
            if (task != null) {
                Operation.Request request = task.task();
                if (request.operation() == response.operation()) {
                    if (result == null) {
                        if (request instanceof Operation.CallRequest
                                && response instanceof Operation.CallResponse) {
                            result = OpCallResult.create(
                                    (Operation.CallRequest) request,
                                    (Operation.CallResponse) response);
                        } else {
                            result = OpResult.create(request, response);
                        }
                    } else {
                        // TODO: checkArgument()
                    }
                    task = tasks.poll();
                    assert (task != null);
                } else {
                    task = null;
                }
            }
        }

        if (task != null) {
            SettableFuture<Operation.Result> future = task.future();
            if (event.operation() == Operation.CREATE_SESSION) {
                if (!(response instanceof Operation.Error)) {
                    future.set(result);
                } else {
                    future.setException(KeeperException
                            .create(((Operation.Error) response).error()));
                }
            } else {
                future.set(result);
            }
        }

        post(SessionResponseEvent.create(session(), event));

        if (event.operation() == Operation.CLOSE_SESSION) {
            disconnected();
        }
    }

    protected void disconnected() {
        connection.unregister(this);
        connection.close();

        synchronized (tasks) {
            Pair<Operation.Request, SettableFuture<Operation.Result>> task = tasks
                    .poll();
            while (task != null) {
                task.second().cancel(true);
            }
        }
    }
}
