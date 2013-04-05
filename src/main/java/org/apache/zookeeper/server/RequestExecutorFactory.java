package org.apache.zookeeper.server;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.RequestExecutorService;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.data.Operations;
import org.apache.zookeeper.event.SessionStateEvent;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.FilteredProcessor;
import org.apache.zookeeper.util.FilteredProcessors;
import org.apache.zookeeper.util.OptionalProcessor;
import org.apache.zookeeper.util.Processor;
import org.apache.zookeeper.util.ProcessorBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class RequestExecutorFactory extends RequestExecutor implements RequestExecutorService, RequestExecutorService.Factory {

    protected static Processor<Operation.Request, Operation.Result> getMainProcessor(
            Zxid zxid,
            SessionManager sessions) {
        @SuppressWarnings("unchecked")
        Processor<Operation.Request, Operation.Response> requestProcessor =
                FilteredProcessors.create(
                        OpCreateSessionProcessor.create(sessions),
                        FilteredProcessor.create(
                                OpRequestProcessor.NotEqualsFilter.create(Operation.CREATE_SESSION), 
                                OpRequestProcessor.create()));
        return getProcessor(zxid, requestProcessor);
    }

    protected static Processor<Operation.Request, Operation.Result> getSessionProcessor(
            Zxid zxid,
            SessionManager sessions,
            long sessionId) {
        @SuppressWarnings("unchecked")
        Processor<Operation.Request, Operation.Response> requestProcessor =
                FilteredProcessors.create(
                        OpCloseSessionProcessor.create(sessionId, sessions),
                        FilteredProcessor.create(
                                OpRequestProcessor.NotEqualsFilter.create(Operation.CLOSE_SESSION), 
                                OpRequestProcessor.create()));
        return getProcessor(zxid, requestProcessor);
    }
    
    protected static Processor<Operation.Request, Operation.Result> getProcessor(Zxid zxid, Processor<Operation.Request, Operation.Response> requestProcessor) {
        requestProcessor = OpErrorProcessor.create(requestProcessor);
        requestProcessor = ProcessorBridge.create(requestProcessor,
                OptionalProcessor.create(AssignZxidProcessor.create(zxid)));
        Processor<Operation.Request, Operation.Result> processor = 
                OpResultProcessor.create(requestProcessor);
        return processor;        
    }
    
    protected final Logger logger = LoggerFactory.getLogger(RequestExecutorFactory.class);
    protected final Provider<Eventful> eventfulFactory;
    protected final Map<Long, RequestExecutorService> executors;
    protected final Zxid zxid;
    protected final SessionManager sessions;
    
    @Inject
    public RequestExecutorFactory(
            Provider<Eventful> eventfulFactory,
            ExecutorService executor, 
            Zxid zxid, SessionManager sessions) {
        this(eventfulFactory, executor, zxid, sessions,
                Collections.synchronizedMap(Maps.<Long, RequestExecutorService>newHashMap()));
    }
    
    protected RequestExecutorFactory(
            Provider<Eventful> eventfulFactory,
            ExecutorService executor, 
            Zxid zxid, SessionManager sessions,
            Map<Long, RequestExecutorService> executors) {
        super(eventfulFactory.get(), executor, getMainProcessor(zxid, sessions));
        this.eventfulFactory = eventfulFactory;
        this.executors = executors;
        this.zxid = zxid;
        this.sessions = sessions;
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
    
    public RequestExecutorService get(Session session) {
        return get(session.id());
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
    
    protected RequestExecutorService newExecutor(long sessionId) {
        return RequestExecutor.create(eventfulFactory.get(), executor(), getSessionProcessor(
                zxid(), sessions(), sessionId));
    }

    @Override
    public RequestExecutorService get() {
        return this;
    }

    @Subscribe
    public void handleEvent(SessionStateEvent event) throws InterruptedException {
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
}
