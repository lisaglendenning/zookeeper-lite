package org.apache.zookeeper.server;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionStateEvent;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Operations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

public class RequestExecutorFactory extends RequestExecutor implements RequestExecutorService, RequestExecutorService.Factory {
    
    protected final Logger logger = LoggerFactory.getLogger(RequestExecutorFactory.class);
    protected final Map<Long, RequestExecutorService> executors;
    
    @Inject
    public RequestExecutorFactory(
            ExecutorService executor, 
            Zxid zxid, SessionManager sessions) {
        this(executor, zxid, sessions,
                Collections.synchronizedMap(Maps.<Long, RequestExecutorService>newHashMap()));
    }
    
    protected RequestExecutorFactory(
            ExecutorService executor, 
            Zxid zxid, SessionManager sessions,
            Map<Long, RequestExecutorService> executors) {
        super(executor, zxid, sessions);
        this.executors = executors;
        sessions.register(this);
    }
    
    protected Map<Long, RequestExecutorService> executors() {
        return executors;
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
        return SessionRequestExecutor.create(executor(), zxid(), sessions(), sessionId);
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
        case CLOSED:
        {
            executors().remove(session.id());
            break;
        }
        case EXPIRED:
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
