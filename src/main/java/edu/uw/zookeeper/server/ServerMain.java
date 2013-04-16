package edu.uw.zookeeper.server;

import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RequestExecutorService;
import edu.uw.zookeeper.ServiceMain;
import edu.uw.zookeeper.Zxid;
import edu.uw.zookeeper.util.ServiceMonitor;

public class ServerMain extends ServiceMain {

    public static void main(String[] args) throws Exception {
        ServerMain main = get();
        main.apply(args);
    }

    public static ServerMain get() {
        return new ServerMain();
    }

    protected ServerMain() {
    }

    @Override
    protected void configure() {
        super.configure();
        bind(Service.class).to(ServiceMonitor.class);
        bind(Zxid.class).in(Singleton.class);
        bind(ExpiringSessionManager.class).in(Singleton.class);
        bind(ExpireSessionsTask.class).in(Singleton.class);
        bind(SessionParametersPolicy.class).to(
                DefaultSessionParametersPolicy.class);
        bind(RequestExecutorService.Factory.class).to(
                SessionRequestExecutor.Factory.class).in(Singleton.class);
        bind(ConnectionManager.class).asEagerSingleton();
        // bind(ExpireSessionsTask.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public SessionManager getSessionManager(ExpiringSessionManager manager,
            ExpireSessionsTask task, ServiceMonitor monitor) {
        monitor.add(task);
        return manager;
    }
}
