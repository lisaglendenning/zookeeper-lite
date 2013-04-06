package org.apache.zookeeper.server;

import static com.google.common.base.Preconditions.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import org.apache.zookeeper.RequestExecutorService;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.SessionConnectionState;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.event.SessionConnectionStateEvent;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.FilteredProcessor;
import org.apache.zookeeper.util.FilteringProcessor;
import org.apache.zookeeper.util.OptionalProcessor;
import org.apache.zookeeper.util.Processor;
import org.apache.zookeeper.util.ProcessorBridge;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class SessionRequestExecutor extends RequestExecutor implements RequestExecutorService, SessionConnection, Callable<ListenableFuture<Operation.Result>> {

    public static class Factory extends RequestExecutor.Factory {

    	public static Factory create(
    			Provider<Eventful> eventfulFactory,
    			ExecutorService executor,
    			SessionManager sessions,
    			Zxid zxid) {
    		return new Factory(eventfulFactory, executor, sessions, zxid);
    	}
    	
    	@Inject
    	protected Factory(
    			Provider<Eventful> eventfulFactory,
    			ExecutorService executor,
    			SessionManager sessions,
    			Zxid zxid) {
    		super(eventfulFactory, executor, sessions, zxid);
    	}

	    @Override
	    protected RequestExecutorService newExecutor(long sessionId) {
	        SessionConnectionState state = SessionConnectionState.create(eventfulFactory.get(), State.CONNECTED);
	        Session session = sessions().get(sessionId);
	        return SessionRequestExecutor.create(
	        		eventfulFactory, 
	        		executor(),
	        		session,
	        		state,
	        		getResponseProcessor(getSessionProcessor(sessionId, state)));
	    }

        protected Processor<Operation.Request, Operation.Response> getSessionProcessor(
                long sessionId,
                SessionConnectionState state) {
            Processor<Operation.Request, Operation.Response> requestProcessor = getSessionProcessor(sessionId);
            requestProcessor = ProcessorBridge.create(requestProcessor, 
                    OptionalProcessor.create(
                            OpCloseSessionStateProcessor.create(state)));
            return requestProcessor;
        }
    }
    
    protected static class OpCloseSessionStateProcessor implements Processor<Operation.Response, Operation.Response> {

        public static FilteringProcessor<Operation.Response, Operation.Response> create(
                SessionConnectionState state) {
            return FilteredProcessor.create(OpRequestProcessor.EqualsFilter.create(Operation.CLOSE_SESSION),
                    new OpCloseSessionStateProcessor(state));
        }

        protected final SessionConnectionState state;
        
        public OpCloseSessionStateProcessor(SessionConnectionState state) {
            this.state = state;
        }

        @Override
        public Operation.Response apply(Operation.Response input) throws Exception {
            if (input.operation() == Operation.CLOSE_SESSION) {
                if (! (input instanceof Operation.Error)) {
                    state.compareAndSet(State.DISCONNECTING, State.DISCONNECTED);
                }
            }
            return input;
        }
        
    }

    public static SessionRequestExecutor create(
            Provider<Eventful> eventfulFactory,
            ExecutorService executor,
            Session session,
            SessionConnectionState state,
            Processor<Operation.Request, Operation.Result> processor) {
        return new SessionRequestExecutor(
                eventfulFactory,
                executor,
                session,
                state,
                processor);
    }

    protected final Session session;
    protected final SessionConnectionState state;

    @Inject
    protected SessionRequestExecutor(
    		Provider<Eventful> eventfulFactory,
    		ExecutorService executor,
    		Session session,
    		SessionConnectionState state,
    		Processor<Operation.Request, Operation.Result> processor) {
        super(eventfulFactory, executor, processor);
        this.session = session;
        this.state = state;
        state.register(this);
    }
    
    public Session session() {
        return session;
    }

    @Override
	public State state() {
	    return state.get();
	}

	@Override
    public ListenableFuture<Operation.Result> submit(Operation.Request request) {
	    checkState(state() == State.CONNECTED);
	    checkNotNull(request);
	    if (request.operation() == Operation.CLOSE_SESSION) {
	    	boolean valid = state.compareAndSet(State.CONNECTED, State.DISCONNECTING);
	    	checkState(valid);
	    }
	    
	    return super.submit(request);
    }
	
	@Subscribe
	public void handleSessionConnectionStateEvent(SessionConnection.State event) {
        post(SessionConnectionStateEvent.create(session(), event));
	}
}