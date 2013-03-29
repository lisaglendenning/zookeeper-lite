package org.apache.zookeeper.client;

import static com.google.common.base.Preconditions.*;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ConnectionEventValue;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionEventValue;
import org.apache.zookeeper.SessionParameters;
import org.apache.zookeeper.SessionState;
import org.apache.zookeeper.SessionStateEvent;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.protocol.OpCreateSessionAction;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Operations;
import org.apache.zookeeper.protocol.Records;
import org.apache.zookeeper.util.ConfigurableTime;
import org.apache.zookeeper.util.Configuration;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.ForwardingEventful;
import org.apache.zookeeper.util.Parameters;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class ClientSession extends ForwardingEventful {

    public static final String PARAM_KEY_TIMEOUT = "Client.Timeout";
    public static final int PARAM_DEFAULT_TIMEOUT = 60;
    public static final String PARAM_KEY_TIMEOUT_UNIT = "Client.TimeoutUnit";
    public static final String PARAM_DEFAULT_TIMEOUT_UNIT = "SECONDS";

    public static final ConfigurableTime TIMEOUT = ConfigurableTime.create(
            PARAM_KEY_TIMEOUT, PARAM_DEFAULT_TIMEOUT, PARAM_KEY_TIMEOUT_UNIT,
            PARAM_DEFAULT_TIMEOUT_UNIT);
    
    public static class Factory implements Provider<ClientSession> {
        
        protected Configuration configuration;
        protected Provider<Eventful> eventfulFactory;
        
        @Inject
        public Factory(Configuration configuration,
                Provider<Eventful> eventfulFactory) {
            this.configuration = configuration;
            this.eventfulFactory = eventfulFactory;
            TIMEOUT.configure(configuration);
        }
        
        public ClientSession get() {
            return ClientSession.create(eventfulFactory);
        }
    }
    
    public static ClientSession create(Provider<Eventful> eventfulFactory) {
        return new ClientSession(eventfulFactory);
    }
    
    public static ClientSession create(Provider<Eventful> eventfulFactory, Zxid zxid) {
        return new ClientSession(eventfulFactory, zxid);
    }
    
    public static ClientSession create(Provider<Eventful> eventfulFactory, Zxid zxid, Session session) {
        return new ClientSession(eventfulFactory, zxid, session);
    }
    
    protected abstract class Task<T> implements Callable<Void> {

        protected SettableFuture<T> future;
        
        protected Task() {
            this.future = SettableFuture.create();
        }
        
        public void cancel() {
            future.cancel(true);
        }
        
        public SettableFuture<T> future() {
            return future;
        }
    }

    protected class RequestTask extends Task<Operation.Result> {

        protected Operation.Request request;
        
        public RequestTask(Operation.Request request) {
            super();
            this.request = request;
            connection().register(this);
        }
        
        public Void call() throws Exception {
            try {
                connection().send(request);
            } catch (Exception e) {
                future.setException(e);
            }
            return null;
        }
        
        @SuppressWarnings("unchecked")
        @Subscribe
        public void handleEvent(ConnectionEventValue<?> event) {
            try {
                if (event.event() instanceof Operation.Result) {
                    Operation.Result result = (Operation.Result)event.event();
                    if (result.operation() == request.operation()) {
                        // TODO: need to unwrap more
                        if (result.response() instanceof Operation.Error) {
                            onFailure(KeeperException.create(((Operation.Error)result.response()).error()));
                        } else {
                            onSuccess(result);
                        }
                    }
                } // TODO: session/connection states
            } catch (Exception e) {
                onFailure(e);
            }
        }
        
        protected void onSuccess(Operation.Result response) {
            connection.unregister(this);
            tasks.remove(this);
            future().set(response);
        }
        
        protected void onFailure(Throwable cause) {
            connection.unregister(this);
            tasks.remove(this);
            future().setException(cause);
        }
    }
    
    protected class ConnectTask extends RequestTask {

        public ConnectTask(OpCreateSessionAction.Request request) {
            super(request);
        }
        
        @Override
        public void onSuccess(Operation.Result message) {
            super.onSuccess(message);
            OpCreateSessionAction.Response response = (OpCreateSessionAction.Response)message.response();
            onOpened(response);
        }
    }

    protected class CloseTask extends RequestTask {

        public CloseTask(Operation.Request request) {
            super(request);
        }
        
        @Override
        public void onSuccess(Operation.Result message) {
            super.onSuccess(message);
            onClosed(message);
        }
    }
    
    protected SessionState state;
    protected Session session;
    protected Connection connection;
    protected Zxid zxid;
    protected List<Task> tasks;

    @Inject
    protected ClientSession(Provider<Eventful> eventfulFactory) {
        this(eventfulFactory, Zxid.create());
    }
    
    protected ClientSession(Provider<Eventful> eventfulFactory, Zxid zxid) {
        this(eventfulFactory, zxid, Session.create());
    }
    
    protected ClientSession(Provider<Eventful> eventfulFactory, Zxid zxid, Session session) {
        this(eventfulFactory, zxid, session, SessionState.create(eventfulFactory.get()));
    }

    protected ClientSession(Provider<Eventful> eventfulFactory, 
            Zxid zxid, 
            Session session, 
            SessionState state) {
        super(eventfulFactory.get());
        this.session = session;
        this.zxid = zxid;
        this.state = state;
        this.tasks = Lists.newArrayList();
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
        this.connection = checkNotNull(connection);
        connection.register(this);
        
        OpCreateSessionAction.Request message = Operations.Requests.create(Operation.CREATE_SESSION);
        ConnectRequest request = message.request();
        request.setProtocolVersion(Records.PROTOCOL_VERSION);
        request.setSessionId(Session.UNINITIALIZED_ID);
        request.setPasswd(SessionParameters.NO_PASSWORD);
        request.setTimeOut((int) TIMEOUT.convert(TimeUnit.MILLISECONDS));
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
        // TODO: cancel tasks?
    }

    public ListenableFuture<Operation.Result> submit(Operation.Request request) throws Exception {
        checkState(connection != null);
        checkState(connection.state() == Connection.State.OPENING
                || connection.state() == Connection.State.OPENED);
        checkState(!state().isTerminal());
        RequestTask task;
        switch (request.operation()) {
        case CREATE_SESSION:
            task = new ConnectTask((OpCreateSessionAction.Request)request);
            break;
        case CLOSE_SESSION:
            task = new CloseTask(request);
            break;
        default:
            task = new RequestTask(request);
            break;
        }
        tasks.add(task);
        task.call();
        return task.future();
    }
    
    public ListenableFuture<Operation.Result> close() throws Exception {
        Operation.Request message = Operations.Requests.create(Operation.CLOSE_SESSION);
        return submit(message);
    }

    @Subscribe
    public void handleEvent(Session.State event) {
        post(SessionStateEvent.create(session(), event));
    }
    
    @Subscribe
    public void handleEvent(ConnectionEventValue<?> event) {
        // TODO: check for errors
        if (event.event() instanceof Operation.CallResponse) {
            handleOperationCallResponseEvent((Operation.CallResponse)event.event());
        }
        post(SessionEventValue.create(session(), event));
    }

    @Subscribe
    public void handleOperationCallResponseEvent(Operation.CallResponse event) {
        synchronized (zxid) {
            long lastZxid = zxid.get();
            if (event.zxid() > lastZxid) {
                zxid.compareAndSet(lastZxid, event.zxid());
            }
        }
    }
    
    protected void onOpened(OpCreateSessionAction.Response response) {
        if (response.response().getSessionId() == Session.UNINITIALIZED_ID) {
            throw new IllegalArgumentException(response.toString());
        }
        this.session = Session.create(response);
        state.set(Session.State.OPENED);
    }
    
    protected void onClosed(Operation.Response response) {
        state.set(Session.State.CLOSED);
    }
}
