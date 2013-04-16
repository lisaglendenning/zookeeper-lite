package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.Connection;
import edu.uw.zookeeper.ServiceMain;
import edu.uw.zookeeper.Xid;
import edu.uw.zookeeper.protocol.client.PingSessionsTask;
import edu.uw.zookeeper.util.ServiceMonitor;

public class ClientMain extends ServiceMain {

    public static void main(String[] args) throws Exception {
        ClientMain main = get();
        main.apply(args);
    }

    public static ClientMain get() {
        return new ClientMain();
    }

    protected ClientMain() {
    }

    @Override
    protected void configure() {
        super.configure();
        bind(Xid.class).in(Singleton.class);
        bind(SingleClientConnectionFactory.class).asEagerSingleton();
        bind(ClientSessionConnection.ConnectionFactory.class).in(
                Singleton.class);
        bind(Connection.class).toProvider(SingleClientConnectionFactory.class);
        bind(ClientSessionConnection.class).toProvider(
                ClientSessionConnection.ConnectionFactory.class);
        bind(ClientSessionConnectionService.class).in(Singleton.class);
        bind(PingSessionsTask.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public Service getService(ClientSessionConnectionService service,
            ServiceMonitor monitor) {
        monitor.add(service);
        return monitor;
    }
}
