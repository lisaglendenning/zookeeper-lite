package org.apache.zookeeper.server;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.RequestExecutorService;
import org.apache.zookeeper.ServiceMain;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.util.Application;
import org.apache.zookeeper.util.ApplicationService;
import org.apache.zookeeper.util.Arguments;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.EventfulEventBus;
import org.apache.zookeeper.util.ServiceMonitor;
import org.apache.zookeeper.util.VerboseThreadFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class ServerMain extends ServiceMain {

    public static void main(String[] args) throws Exception {
        ServerMain main = get();
        main.apply(args);
    }

    public static ServerMain get() {
        return new ServerMain();
    }

    protected ServerMain() {}
    
    @Override 
    protected void configure() {
        super.configure();
        bind(Service.class).to(ServiceMonitor.class);
        bind(Zxid.class).in(Singleton.class);
        bind(ExpiringSessionManager.class).in(Singleton.class);
        bind(ExpireSessionsTask.class).in(Singleton.class);
        bind(SessionParametersPolicy.class).to(DefaultSessionParametersPolicy.class);
        bind(RequestExecutorService.Factory.class).to(SessionRequestExecutor.Factory.class).in(Singleton.class);
        bind(ConnectionManager.class).asEagerSingleton();
        //bind(ExpireSessionsTask.class).asEagerSingleton();
    }
    
    @Provides @Singleton
    public SessionManager getSessionManager(ExpiringSessionManager manager,
    		ExpireSessionsTask task,
            ServiceMonitor monitor) {
        monitor.add(task);
        return manager;
    }
}
