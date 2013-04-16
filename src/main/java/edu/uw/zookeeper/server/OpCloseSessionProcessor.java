package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.util.FilteredProcessor;
import edu.uw.zookeeper.util.FilteringProcessor;

public class OpCloseSessionProcessor extends OpRequestProcessor {

    public static FilteringProcessor<Operation.Request, Operation.Response> create(
            long sessionId, SessionManager sessions) {
        return FilteredProcessor.create(
                EqualsFilter.create(Operation.CLOSE_SESSION),
                new OpCloseSessionProcessor(sessionId, sessions));
    }

    protected final Logger logger = LoggerFactory
            .getLogger(OpCloseSessionProcessor.class);
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
