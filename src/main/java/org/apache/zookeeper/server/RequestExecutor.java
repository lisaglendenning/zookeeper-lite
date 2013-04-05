package org.apache.zookeeper.server;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.RequestExecutorService;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.data.Operations;
import org.apache.zookeeper.event.SessionStateEvent;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.FilteredProcessor;
import org.apache.zookeeper.util.FilteredProcessors;
import org.apache.zookeeper.util.ForwardingEventful;
import org.apache.zookeeper.util.OptionalProcessor;
import org.apache.zookeeper.util.Pair;
import org.apache.zookeeper.util.Processor;
import org.apache.zookeeper.util.ProcessorBridge;
import org.apache.zookeeper.util.SettableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class RequestExecutor extends ForwardingEventful implements RequestExecutorService, Callable<ListenableFuture<Operation.Result>> {

    public static RequestExecutorService create(Eventful eventful, ExecutorService executor, Processor<Operation.Request, Operation.Result> processor) {
        return new RequestExecutor(eventful, executor, processor);
    }
    
    public static class Factory implements RequestExecutorService.Factory {

        protected final Map<Long, RequestExecutorService> executors;
    	protected final Provider<Eventful> eventfulFactory;
    	protected final ExecutorService executor;
    	protected final Zxid zxid;
    	protected final SessionManager sessions;
    	protected final RequestExecutorService anonymousExecutor;
    	
    	@Inject
    	protected Factory(
    			Provider<Eventful> eventfulFactory,
    			ExecutorService executor,
    			SessionManager sessions,
    			Zxid zxid) {
    		this(eventfulFactory, executor, sessions, zxid,
    				Collections.synchronizedMap(Maps.<Long, RequestExecutorService>newHashMap()));
    	}

    	protected Factory(
    			Provider<Eventful> eventfulFactory,
    			ExecutorService executor,
    			SessionManager sessions,
    			Zxid zxid,
                Map<Long, RequestExecutorService> executors) {
    		this.executors = executors;
    		this.eventfulFactory = eventfulFactory;
    		this.executor = executor;
    		this.zxid = zxid;
    		this.sessions = sessions;
    		this.anonymousExecutor = newExecutor();
            sessions.register(this);
    	}
    	
        protected Map<Long, RequestExecutorService> executors() {
            return executors;
        }
        
        public Zxid zxid() {
            return zxid;
        }

        public SessionManager sessions() {
            return sessions;
        }
        
        public ExecutorService executor() {
        	return executor;
        }
        
        public RequestExecutorService get(Session session) {
            return get(session.id());
        }
        
        @Override
		public RequestExecutorService get() {
		    return anonymousExecutor;
		}

		@Override
		public RequestExecutorService get(long sessionId) {
	        RequestExecutorService executor;
	        synchronized (executors()) {
	            executor = executors().get(sessionId);
	            if (executor == null && sessions.get(sessionId) != null) {
	                executor = newExecutor(sessionId);
	                executors().put(sessionId, executor);
	            }
	        }
	        return executor;
		}

	    @Subscribe
	    public void handleEvent(SessionStateEvent event) {
	        Session session = event.session();
	        RequestExecutorService executor = get(session);
	        switch (event.event()) {
	        case SESSION_CLOSED:
	        {
	            executors().remove(session.id());
	            break;
	        }
	        case SESSION_EXPIRED:
	        {
	            // here is where the session closing is actually initiated
	            // there's no point sending the close response
	            // to the client because it doesn't have the context for
	            // the message (no xid!)
	            if (executor != null) {
	                Operation.Request request = Operations.Requests.create(Operation.CLOSE_SESSION);
	                executor.submit(request);
	            }
	            break;
	        }
	        default:
	            break;
	        }
	    }

	    protected RequestExecutorService newExecutor() {
	        return RequestExecutor.create(
	        		eventfulFactory.get(), 
	        		executor(), 
	        		getAnonymousProcessor());
	    }
	    
	    protected RequestExecutorService newExecutor(long sessionId) {
	        return RequestExecutor.create(
	        		eventfulFactory.get(), 
	        		executor(), 
	        		getSessionProcessor(sessionId));
	    }
	    
		protected Processor<Operation.Request, Operation.Result> getAnonymousProcessor() {
            @SuppressWarnings("unchecked")
            Processor<Operation.Request, Operation.Response> requestProcessor =
                    FilteredProcessors.create(
                            OpCreateSessionProcessor.create(sessions),
                            FilteredProcessor.create(
                                    OpRequestProcessor.NotEqualsFilter.create(Operation.CREATE_SESSION), 
                                    OpRequestProcessor.create()));
            return getResponseProcessor(requestProcessor);
        }

        protected Processor<Operation.Request, Operation.Result> getSessionProcessor(long sessionId) {
            @SuppressWarnings("unchecked")
            Processor<Operation.Request, Operation.Response> requestProcessor =
                    FilteredProcessors.create(
                            OpCloseSessionProcessor.create(sessionId, sessions),
                            FilteredProcessor.create(
                                    OpRequestProcessor.NotEqualsFilter.create(Operation.CLOSE_SESSION), 
                                    OpRequestProcessor.create()));
            return getResponseProcessor(requestProcessor);
        }

        protected Processor<Operation.Request, Operation.Result> getResponseProcessor(Processor<Operation.Request, Operation.Response> requestProcessor) {
            requestProcessor = OpErrorProcessor.create(requestProcessor);
            requestProcessor = ProcessorBridge.create(requestProcessor,
                    OptionalProcessor.create(AssignZxidProcessor.create(zxid)));
            Processor<Operation.Request, Operation.Result> processor = 
                    OpResultProcessor.create(requestProcessor);
            return processor;        
        }
        
    }
    
    protected final Logger logger = LoggerFactory.getLogger(RequestExecutor.class);
    protected final BlockingQueue<SettableTask<Operation.Request, Operation.Result>> requests;
    protected final Processor<Operation.Request, Operation.Result> processor;
    protected final ExecutorService executor;

    @Inject
    public RequestExecutor(Eventful eventful, ExecutorService executor, Processor<Operation.Request, Operation.Result> processor) {
        super(eventful);
        this.requests = new LinkedBlockingQueue<SettableTask<Operation.Request, Operation.Result>>();
        this.executor = executor;
        this.processor = processor;
    }
    
    protected ExecutorService executor() {
        return executor;
    }
    
    protected Processor<Operation.Request, Operation.Result> processor() {
        return processor;
    }
    
    protected BlockingQueue<SettableTask<Operation.Request, Operation.Result>> requests() {
        return requests;
    }

    @Override
    public ListenableFuture<Operation.Result> submit(Operation.Request request) {
        logger.debug("Submitting request {}", request);
        SettableTask<Operation.Request, Operation.Result> task = SettableTask.create(request);
        requests().add(task);
        executor().submit(this);
        return task.future();
    }
    
    public ListenableFuture<Operation.Result> call() throws Exception {
        Pair<Operation.Request, SettableFuture<Operation.Result>> request = requests().poll();
        if (request != null) {
            Operation.Result result = null;
            try {
                result = processor().apply(request.first());
            } catch (Throwable t) {
                request.second().setException(t);
                return request.second();
            }
            request.second().set(result);
            return request.second();
        } else {
            return null;
        }
    }
}
