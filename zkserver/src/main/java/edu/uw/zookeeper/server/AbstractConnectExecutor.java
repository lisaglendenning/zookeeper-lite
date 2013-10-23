package edu.uw.zookeeper.server;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import net.engio.mbassy.PubSubSupport;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.ZxidReference;

public abstract class AbstractConnectExecutor extends PolicySessionManager implements TaskExecutor<Pair<ConnectMessage.Request, ? extends PubSubSupport<Object>>, ConnectMessage.Response> {

    protected final Logger logger;
    protected final PubSubSupport<? super SessionEvent> publisher;
    protected final ConnectMessageProcessor processor;
    protected final SessionParametersPolicy policy;
    
    protected AbstractConnectExecutor(
            PubSubSupport<? super SessionEvent> publisher,
            SessionParametersPolicy policy,
            ZxidReference lastZxid) {
        this.logger = LogManager.getLogger(getClass());
        this.publisher = publisher;
        this.policy = policy;
        this.processor = ConnectMessageProcessor.create(this, lastZxid);
    }

    @Override
    public SessionParametersPolicy policy() {
        return policy;
    }

    @Override
    public Session get(long id) {
        SessionExecutor session = sessions().get(id);
        if (session != null) {
            return session.session();
        }
        return null;
    }

    @Override
    public Iterator<Session> iterator() {
        return Iterators.transform(sessions().values().iterator(), 
                new Function<SessionExecutor, Session>() {
            @Override
            public Session apply(SessionExecutor input) {
                return input.session();
            }
        });
    }

    @Override
    protected Session doRemove(long id) {
        SessionExecutor session = sessions().remove(id);
        if (session != null) {
            return session.session();
        }
        return null;
    }

    @Override
    protected PubSubSupport<? super SessionEvent> publisher() {
        return publisher;
    }

    @Override
    protected Logger logger() {
        return logger;
    }
    
    protected abstract ConcurrentMap<Long, ? extends SessionExecutor> sessions();
}
