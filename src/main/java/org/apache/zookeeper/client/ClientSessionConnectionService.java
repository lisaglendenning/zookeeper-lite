package org.apache.zookeeper.client;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class ClientSessionConnectionService extends AbstractIdleService
        implements Provider<ClientSessionConnection> {

    public static ClientSessionConnectionService create(
            Provider<ClientSessionConnection> sessionFactory) {
        return new ClientSessionConnectionService(sessionFactory);
    }

    protected final Provider<ClientSessionConnection> sessionFactory;
    protected ClientSessionConnection session;

    @Inject
    protected ClientSessionConnectionService(
            Provider<ClientSessionConnection> sessionFactory) {
        this.sessionFactory = sessionFactory;
        this.session = null;
    }

    @Override
    protected void startUp() throws Exception {
        session = sessionFactory.get();
        get().connect().get();
    }

    @Override
    protected void shutDown() throws Exception {
        get().disconnect().get();
    }

    @Override
    public ClientSessionConnection get() {
        return session;
    }
}
