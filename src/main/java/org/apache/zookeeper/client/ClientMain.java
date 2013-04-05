package org.apache.zookeeper.client;

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

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.Xid;
import org.apache.zookeeper.protocol.client.PingSessionsTask;
import org.apache.zookeeper.util.Application;
import org.apache.zookeeper.util.ApplicationService;
import org.apache.zookeeper.util.Arguments;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.EventfulEventBus;
import org.apache.zookeeper.util.Main;
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

public class ClientMain extends Main {

    public static void main(String[] args) throws Exception {
        ClientMain main = get();
        main.apply(args);
    }

    public static ClientMain get() {
        return new ClientMain();
    }

    protected ClientMain() {}
    
    @Override
    protected void apply(String[] args) throws Exception {
        Arguments arguments = getArguments(this);
        arguments.setArgs(args);
        Injector injector = getInjector(modules());
        Application app = injector.getInstance(Application.class);
        arguments.parse();
        if (arguments.helpOptionSet()) {
            System.out.println(arguments.getUsage());
            System.exit(0);
        }
        app.call();
    }

    @Override
    protected List<Module> modules() {
        return Lists.<Module>newArrayList();
    }

    @Override 
    protected void configure() {
        super.configure();
        bind(ThreadFactory.class).to(VerboseThreadFactory.class).in(Singleton.class);
        bind(Executor.class).to(ExecutorService.class).in(Singleton.class);
        bind(Eventful.class).to(EventfulEventBus.class);
        bind(ServiceMonitor.class).in(Singleton.class);
        bind(ApplicationService.class).in(Singleton.class);
        bind(Application.class).to(ApplicationService.class);
        bind(Xid.class).in(Singleton.class);
        bind(SingleClientConnectionFactory.class).in(Singleton.class);
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

    @Provides @Singleton
    public ExecutorService executorService(ThreadFactory threads) {
        int corePoolSize = 4;
        int maxPoolSize = 20;
        long keepAlive = 1000;
        TimeUnit keepAliveUnit = TimeUnit.MILLISECONDS;
        BlockingQueue<Runnable> queue = new SynchronousQueue<Runnable>();
        ExecutorService executor = new ThreadPoolExecutor(
                corePoolSize, maxPoolSize, 
                keepAlive, keepAliveUnit, 
                queue, threads);
        return executor;
    }
    
    @Provides @Singleton
    public ScheduledExecutorService scheduledExecutorService(ThreadFactory threads) {
        int corePoolSize = 4;
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(corePoolSize, threads);
        return executor;
    }

    @Provides @Singleton
    public ListeningExecutorService listeningExecutorService(ExecutorService executor) {
        return MoreExecutors.listeningDecorator(executor);
    }
}
