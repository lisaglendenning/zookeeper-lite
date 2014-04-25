package edu.uw.zookeeper.server;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.server.SessionExecutor;

public class SimpleSessionManager<T extends SessionExecutor> extends AbstractSessionManager {

    public static <T extends SessionExecutor> SimpleSessionManager<T> fromConfiguration(
            short id,
            ConcurrentMap<Long, T> sessions,
            ParameterizedFactory<? super Session, ? extends T> factory,
            Configuration configuration) {
        DefaultSessionParametersPolicy policy = DefaultSessionParametersPolicy.fromConfiguration(id, configuration);
        return create(factory, sessions, policy);
    }
    
    public static <T extends SessionExecutor> SimpleSessionManager<T> create(
            ParameterizedFactory<? super Session, ? extends T> factory,
            Map<Long, T> sessions,
            SessionParametersPolicy policy) {
        return new SimpleSessionManager<T>(factory, sessions, policy);
    }
    
    protected final Logger logger;
    protected final Map<Long, T> executors;
    protected final ParameterizedFactory<? super Session, ? extends T> factory;
    
    protected SimpleSessionManager(
            ParameterizedFactory<? super Session, ? extends T> factory,
            Map<Long, T> sessions,
            SessionParametersPolicy policy) {
        super(policy);
        this.logger = LogManager.getLogger(getClass());
        this.factory = factory;
        this.executors = sessions;
    }
    
    public Map<Long, T> executors() {
        return executors;
    }

    @Override
    public Session remove(long id) {
        SessionExecutor executor = executors.remove(Long.valueOf(id));
        if (executor != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Removed session {}", Session.toString(id));
            }
            return executor.session();
        }
        return null;
    }

    @Override
    public Session get(long id) {
        T executor = executors.get(Long.valueOf(id));
        return (executor != null) ? executor.session() : null;
    }

    @Override
    public Session put(Session session) {
        Long k = Long.valueOf(session.id());
        T existing = executors.get(k);
        if (existing != null) {
            if (existing.session().equals(session)) {
                return null;
            } else {
                throw new IllegalArgumentException(String.valueOf(session));
            }
        }
        existing = executors.put(k, factory.get(session));
        if (existing != null) {
            throw new IllegalStateException();
        }
        return null;
    }

    @Override
    public Iterator<Session> iterator() {
        return Iterators.transform(executors.values().iterator(), 
                new Function<T, Session>() {
            @Override
            public Session apply(T input) {
                return input.session();
            }
        });
    }
}