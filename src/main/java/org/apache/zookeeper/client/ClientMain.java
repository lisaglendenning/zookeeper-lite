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

import org.apache.zookeeper.Xid;
import org.apache.zookeeper.client.Client;
import org.apache.zookeeper.protocol.client.PingSessionsTask;
import org.apache.zookeeper.util.Application;
import org.apache.zookeeper.util.ApplicationService;
import org.apache.zookeeper.util.Arguments;
import org.apache.zookeeper.util.Configuration;
import org.apache.zookeeper.util.EventfulEventBus;
import org.apache.zookeeper.util.Main;
import org.apache.zookeeper.util.ServiceMonitor;
import org.apache.zookeeper.util.VerboseThreadFactory;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.internal.Lists;

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
        return Lists.<Module>newArrayList(
                EventfulEventBus.EventfulModule.get(),
                ApplicationService.ApplicationModule.get());
    }

    @Override 
    protected void configure() {
        bind(ServiceMonitor.class).in(Singleton.class);
    }
    
    @Provides
    public ClientSession getClientSession(ClientSession.Factory factory) {
        return factory.get();
    }
    
    @Provides @Singleton
    public Client getClient(Client.Factory factory, ClientSession session) {
        return factory.newClient(session);
    }

    @Provides @Singleton
    public Service getService(Client client, PingSessionsTask pinging, ServiceMonitor monitor) {
        monitor.add(client);
        return monitor;
    }

    @Provides @Singleton
    public Xid xid() {
        return Xid.create();
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
    public Executor executor(ExecutorService executor) {
        return executor;
    }

    @Provides @Singleton
    public ListeningExecutorService listeningExecutorService(ExecutorService executor) {
        return MoreExecutors.listeningDecorator(executor);
    }
    
    @Provides @Singleton
    public ThreadFactory threadFactory() {
        return new VerboseThreadFactory();
    }
}
