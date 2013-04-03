package org.apache.zookeeper.client;

import static com.google.common.base.Preconditions.*;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ConnectionEventValue;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.RequestExecutorService;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionEventValue;
import org.apache.zookeeper.SessionParameters;
import org.apache.zookeeper.SessionState;
import org.apache.zookeeper.SessionStateEvent;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.protocol.OpCallResult;
import org.apache.zookeeper.protocol.OpCreateSessionAction;
import org.apache.zookeeper.protocol.OpResult;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Operations;
import org.apache.zookeeper.protocol.Records;
import org.apache.zookeeper.util.ConfigurableTime;
import org.apache.zookeeper.util.Configuration;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.ForwardingEventful;
import org.apache.zookeeper.util.Pair;
import org.apache.zookeeper.util.SettableTask;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class ClientSession extends ForwardingEventful implements RequestExecutorService {

    public static class Factory implements Provider<ClientSession> {

        public static final String PARAM_KEY_TIMEOUT = "Client.Timeout";
        public static final int PARAM_DEFAULT_TIMEOUT = 60;
        public static final String PARAM_KEY_TIMEOUT_UNIT = "Client.TimeoutUnit";
        public static final String PARAM_DEFAULT_TIMEOUT_UNIT = "SECONDS";

        protected final ConfigurableTime timeOut;
        protected Configuration configuration;
        protected Provider<Eventful> eventfulFactory;
        
        @Inject
        public Factory(Configuration configuration,
                Provider<Eventful> eventfulFactory) {
            this.configuration = configuration;
            this.eventfulFactory = eventfulFactory;
            this.timeOut = ConfigurableTime.create(
                    PARAM_KEY_TIMEOUT, PARAM_DEFAULT_TIMEOUT, PARAM_KEY_TIMEOUT_UNIT,
                    PARAM_DEFAULT_TIMEOUT_UNIT);;
            this.timeOut.configure(configuration);
        }
        
        public ClientSession get() {
            return ClientSession.create(eventfulFactory, timeOut);
        }
    }
    
    public static ClientSession create(Provider<Eventful> eventfulFactory, ConfigurableTime timeOut) {
        return new ClientSession(eventfulFactory, timeOut);
    }
    
    public static ClientSession create(Provider<Eventful> eventfulFactory, ConfigurableTime timeOut, Zxid zxid) {
        return new ClientSession(eventfulFactory, timeOut, zxid);
    }
    
    public static ClientSession create(Provider<Eventful> eventfulFactory, ConfigurableTime timeOut, Zxid zxid, Session session) {
        return new ClientSession(eventfulFactory, timeOut, zxid, session);
    }
    
    protected ConfigurableTime timeOut;
    protected SessionState state;
    protected Session session;
    protected Connection connection;
    protected Zxid zxid;
    protected BlockingQueue<SettableTask<Operation.Request, Operation.Result>> tasks;

    @Inject
    protected ClientSession(Provider<Eventful> eventfulFactory, ConfigurableTime timeOut) {
        this(eventfulFactory, timeOut, Zxid.create());
    }
    
    protected ClientSession(Provider<Eventful> eventfulFactory, ConfigurableTime timeOut, Zxid zxid) {
        this(eventfulFactory, timeOut, zxid, Session.create());
    }
    
    protected ClientSession(Provider<Eventful> eventfulFactory, ConfigurableTime timeOut, Zxid zxid, Session session) {
        this(eventfulFactory, timeOut, zxid, session, SessionState.create(eventfulFactory.get()));
    }

    protected ClientSession(
            Provider<Eventful> eventfulFactory, 
            ConfigurableTime timeOut,
            Zxid zxid, 
            Session session, 
            SessionState state) {
        super(eventfulFactory.get());
        this.timeOut = timeOut;
        this.session = session;
        this.zxid = zxid;
        this.state = state;
        this.tasks = new LinkedBlockingQueue<SettableTask<Operation.Request, Operation.Result>>();
        this.connection = null;
        state.register(this);
    }
    
    public Connection connection() {
        return connection;
    }
    
    public Session session() {
        return session;
    }
    
    public Session.State state() {
        return state.get();
    }
    
    public ListenableFuture<Operation.Result> connect(Connection connection) throws Exception {
        checkState(connection.state() == Connection.State.OPENING
                || connection.state() == Connection.State.OPENED);
        checkState(this.connection == null);
        this.connection = checkNotNull(connection);
        connection.register(this);
        
        OpCreateSessionAction.Request message = Operations.Requests.create(Operation.CREATE_SESSION);
        ConnectRequest request = message.record();
        request.setProtocolVersion(Records.PROTOCOL_VERSION);
        request.setSessionId(Session.UNINITIALIZED_ID);
        request.setPasswd(SessionParameters.NO_PASSWORD);
        request.setTimeOut((int) timeOut.convert(TimeUnit.MILLISECONDS));
        request.setLastZxidSeen(zxid.get());
        if (session().id() != Session.UNINITIALIZED_ID) {
            request.setSessionId(session().id());
            request.setPasswd(session().parameters().password());
        }
        
        return submit(message);
    }
    
    public void disconnect() {
        if (connection != null) {
            connection.unregister(this);
            this.connection = null;
        }
        synchronized (tasks) {
            Pair<Operation.Request, SettableFuture<Operation.Result>> task = tasks.poll();
            while (task != null) {
                task.second().cancel(true);
            }
        }
    }

    @Override
    public ListenableFuture<Operation.Result> submit(Operation.Request request) throws InterruptedException {
        checkState(connection != null);
        checkState(connection.state() == Connection.State.OPENING
                || connection.state() == Connection.State.OPENED);
        checkState(!state().isTerminal());
        SettableTask<Operation.Request, Operation.Result> task = SettableTask.create(request);
        tasks.put(task);
        connection.send(task.task());
        return task.future();
    }
    
    public ListenableFuture<Operation.Result> close() throws Exception {
        Operation.Request message = Operations.Requests.create(Operation.CLOSE_SESSION);
        return submit(message);
    }

    @Subscribe
    public void handleEvent(Session.State event) {
        post(event);
    }
    
    @Subscribe
    public void handleEvent(ConnectionEventValue<?> event) {
        Object value = event.event();
        // TODO: other events?
        if (value instanceof Operation.Response) {
            handleEvent((Operation.Response)value);
        }
        post(event);
    }

    @Subscribe
    public void handleEvent(Operation.Response event) {
        Operation.Response response = event;
        Operation.Result result = null;
        if (event instanceof Operation.Result) {
            result = (Operation.Result) event;
            response = result.response();
        }
        
        if (event instanceof Operation.CallResponse) {
            synchronized (zxid) {
                long eventZxid = ((Operation.CallResponse)event).zxid();
                long lastZxid = zxid.get();
                if (eventZxid > lastZxid) {
                    zxid.compareAndSet(lastZxid, eventZxid);
                }
            }
        }
        
        switch (event.operation()) {
        case CREATE_SESSION:
            onOpened((OpCreateSessionAction.Response)response);
            break;
        case CLOSE_SESSION:
            onClosed(response);
            break;
        default:
            break;
        }
        
        SettableTask<Operation.Request, Operation.Result> task = tasks.peek();
        if (task != null) {
            Operation.Request request = task.task();
            SettableFuture<Operation.Result> future = task.future();
            if (request.operation() == response.operation()) {
                if (result == null) {
                    if (request instanceof Operation.CallRequest
                            && response instanceof Operation.CallResponse) {
                        result = OpCallResult.create((Operation.CallRequest) request,
                                (Operation.CallResponse) response);
                    } else {
                        result = OpResult.create(request, response);
                    }
                } else {
                    // TODO: checkArgument()
                }
                task = tasks.poll();
                assert (task != null);
                future.set(result);
            }
        }

        post(event);
    }
    
    protected void onOpened(OpCreateSessionAction.Response response) {
        if (response.isValid()) {
            this.session = Session.create(response);
            state.set(Session.State.OPENED);
        } else {
            throw new IllegalArgumentException(response.toString());
        }
    }
    
    protected void onClosed(Operation.Response response) {
        state.set(Session.State.CLOSED);
    }
}
