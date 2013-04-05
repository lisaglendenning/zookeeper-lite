package org.apache.zookeeper.client;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.RequestExecutorService;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.SessionConnectionState;
import org.apache.zookeeper.SessionParameters;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.data.OpCallResult;
import org.apache.zookeeper.data.OpCreateSessionAction;
import org.apache.zookeeper.data.OpResult;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.data.Operations;
import org.apache.zookeeper.event.ConnectionMessageEvent;
import org.apache.zookeeper.event.SessionConnectionStateEvent;
import org.apache.zookeeper.event.SessionResponseEvent;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.protocol.Records;
import org.apache.zookeeper.util.ConfigurableTime;
import org.apache.zookeeper.util.Configuration;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.ForwardingEventful;
import org.apache.zookeeper.util.Pair;
import org.apache.zookeeper.util.SettableTask;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class ClientSessionConnection extends ForwardingEventful implements RequestExecutorService, SessionConnection {

    public static class Factory {

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
        
        public ClientSessionConnection get(Connection connection) {
            return ClientSessionConnection.create(connection, eventfulFactory, timeOut);
        }
    }
    
    public static ClientSessionConnection create(
    		Connection connection,
    		Provider<Eventful> eventfulFactory,
    		ConfigurableTime timeOut) {
        return new ClientSessionConnection(connection, eventfulFactory, timeOut);
    }
    
    public static ClientSessionConnection create(
    		Connection connection,
    		Provider<Eventful> eventfulFactory, 
    		ConfigurableTime timeOut, 
    		Zxid zxid) {
        return new ClientSessionConnection(connection, eventfulFactory, timeOut, zxid);
    }
    
    public static ClientSessionConnection create(
    		Connection connection,
    		Provider<Eventful> eventfulFactory,
    		ConfigurableTime timeOut,
    		Zxid zxid,
    		Session session) {
        return new ClientSessionConnection(connection, eventfulFactory, timeOut, zxid, session);
    }

    protected Session session;
    protected final Connection connection;
    protected final SessionConnectionState state;
    protected final ConfigurableTime timeOut;
    protected final Zxid zxid;
    protected final BlockingQueue<SettableTask<Operation.Request, Operation.Result>> tasks;

    @Inject
    protected ClientSessionConnection(
    		Connection connection,
    		Provider<Eventful> eventfulFactory,
    		ConfigurableTime timeOut) {
        this(connection, eventfulFactory, timeOut, Zxid.create());
    }
    
    protected ClientSessionConnection(
    		Connection connection,
    		Provider<Eventful> eventfulFactory,
    		ConfigurableTime timeOut,
    		Zxid zxid) {
        this(connection, eventfulFactory, timeOut, zxid, Session.create());
    }
    
    protected ClientSessionConnection(
    		Connection connection,
            Provider<Eventful> eventfulFactory, 
            ConfigurableTime timeOut,
            Zxid zxid, 
            Session session) {
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
        checkState(valid);
        
        state.register(this);
        connection.register(this);
        
        OpCreateSessionAction.Request message = Operations.Requests.create(Operation.CREATE_SESSION);
        ConnectRequest request = message.record();
        request.setProtocolVersion(Records.PROTOCOL_VERSION);
        request.setTimeOut((int) timeOut.convert(TimeUnit.MILLISECONDS));
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
    	boolean valid = state.compareAndSet(State.CONNECTED, State.DISCONNECTING);
        checkState(valid);
        
	    Operation.Request message = Operations.Requests.create(Operation.CLOSE_SESSION);
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
		
	    SettableTask<Operation.Request, Operation.Result> task = SettableTask.create(request);
	    // ensure that tasks are sent in the same as the order of this queue...
    	synchronized (this) {
    		tasks.add(task);
    	    try {
    		connection.send(task.task()); 
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
	        handleEvent((Operation.Response)value);
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
	    case CREATE_SESSION:
	    {
	        if (! (response instanceof Operation.Error)) {
		        OpCreateSessionAction.Response opResponse = (OpCreateSessionAction.Response)response;
	            this.session = Session.create(opResponse);
	            this.state.set(SessionConnection.State.CONNECTED);
	        }
	        break;
	    }
	    case CLOSE_SESSION:
	    {
	    	// TODO: need to unwrap?!
	    	if (! (response instanceof Operation.Error)) {
	    		this.state.set(SessionConnection.State.DISCONNECTED);
	    	}
	        break;
	    }
	    default:
	        break;
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
	    
	    SettableTask<Operation.Request, Operation.Result> task = null;
	    synchronized (this) {
		    task = tasks.peek();
		    if (task != null) {
		        Operation.Request request = task.task();
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
		        }
		    }
	    }
		
	    if (task != null) {
	        SettableFuture<Operation.Result> future = task.future();
            if (event.operation() == Operation.CREATE_SESSION) {
                if (! (response instanceof Operation.Error)) {
                    future.set(result);
                } else {
                    future.setException(KeeperException.create(((Operation.Error)response).error()));
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
            Pair<Operation.Request, SettableFuture<Operation.Result>> task = tasks.poll();
            while (task != null) {
                task.second().cancel(true);
            }
        }
    }
}
