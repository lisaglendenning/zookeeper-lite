package org.apache.zookeeper.server;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.util.FilteredProcessor;
import org.apache.zookeeper.util.FilteringProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;


public class OpCloseSessionProcessor extends OpRequestProcessor {

    public static FilteringProcessor<Operation.Request, Operation.Response> create(
            long sessionId, 
            SessionManager sessions) {
        return FilteredProcessor.create(EqualsFilter.create(Operation.CLOSE_SESSION),
                new OpCloseSessionProcessor(sessionId, sessions));
    }
    
    protected final Logger logger = LoggerFactory.getLogger(OpCloseSessionProcessor.class);
    protected final long sessionId;
    protected final SessionManager sessions;
    
    @Inject
    protected OpCloseSessionProcessor(long sessionId, SessionManager sessions) {
        this.sessionId = sessionId;
        this.sessions = sessions;
    }

    public long sessionId() {
        return sessionId;
    }
    
    public SessionManager sessions() {
        return sessions;
    }

    @Override
    public Operation.Response apply(Operation.Request request) throws Exception {
        checkArgument(request.operation() == Operation.CLOSE_SESSION);
        if (sessions().remove(sessionId()) == null) {
            throw new KeeperException.SessionExpiredException();
        }
        return super.apply(request);
    }
}
