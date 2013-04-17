package edu.uw.zookeeper;

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


import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.ApplicationService;
import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.EventfulEventBus;
import edu.uw.zookeeper.util.Main;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.VerboseThreadFactory;

public class ServiceMain extends Main {

    public static void main(String[] args) throws Exception {
        ServiceMain main = get();
        main.apply(args);
    }

    public static ServiceMain get() {
        return new ServiceMain();
    }

    protected ServiceMain() {
    }

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
        return Lists.<Module> newArrayList();
    }

    @Override
    protected void configure() {
        super.configure();
        bind(ThreadFactory.class).to(VerboseThreadFactory.class).in(
                Singleton.class);
        bind(Executor.class).to(ExecutorService.class).in(Singleton.class);
        bind(Eventful.class).to(EventfulEventBus.class);
        bind(ServiceMonitor.class).in(Singleton.class);
        bind(Application.class).to(ApplicationService.class)
                .in(Singleton.class);
    }

    @Provides
    @Singleton
    public ExecutorService executorService(ThreadFactory threads) {
        int corePoolSize = 4;
        int maxPoolSize = 20;
        long keepAlive = 1000;
        TimeUnit keepAliveUnit = TimeUnit.MILLISECONDS;
        BlockingQueue<Runnable> queue = new SynchronousQueue<Runnable>();
        ExecutorService executor = new ThreadPoolExecutor(corePoolSize,
                maxPoolSize, keepAlive, keepAliveUnit, queue, threads);
        return executor;
    }

    @Provides
    @Singleton
    public ScheduledExecutorService scheduledExecutorService(
            ThreadFactory threads) {
        int corePoolSize = 4;
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(
                corePoolSize, threads);
        return executor;
    }

    @Provides
    @Singleton
    public ListeningExecutorService listeningExecutorService(
            ExecutorService executor) {
        return MoreExecutors.listeningDecorator(executor);
    }
}