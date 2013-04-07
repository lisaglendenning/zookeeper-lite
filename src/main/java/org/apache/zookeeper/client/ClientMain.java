package org.apache.zookeeper.client;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ServiceMain;
import org.apache.zookeeper.Xid;
import org.apache.zookeeper.protocol.client.PingSessionsTask;
import org.apache.zookeeper.util.ServiceMonitor;
import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class ClientMain extends ServiceMain {

    public static void main(String[] args) throws Exception {
        ClientMain main = get();
        main.apply(args);
    }

    public static ClientMain get() {
        return new ClientMain();
    }

    protected ClientMain() {}
    
    @Override 
    protected void configure() {
        super.configure();
        bind(Xid.class).in(Singleton.class);
        bind(SingleClientConnectionFactory.class).asEagerSingleton();
        bind(ClientSessionConnection.ConnectionFactory.class).in(Singleton.class);
        bind(Connection.class).toProvider(SingleClientConnectionFactory.class);
        bind(ClientSessionConnection.class).toProvider(ClientSessionConnection.ConnectionFactory.class);
        bind(ClientSessionConnectionService.class).in(Singleton.class);
        bind(PingSessionsTask.class).asEagerSingleton();
    }
    
    @Provides @Singleton
    public Service getService(ClientSessionConnectionService service, ServiceMonitor monitor) {
    	monitor.add(service);
    	return monitor;
    }
}
