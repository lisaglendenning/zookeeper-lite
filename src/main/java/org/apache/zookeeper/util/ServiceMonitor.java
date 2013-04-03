package org.apache.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;

public class ServiceMonitor extends AbstractIdleService {

    public static ServiceMonitor create(Executor executor) {
        return new ServiceMonitor(executor);
    }
    
    protected static class ServiceMonitorListener implements Service.Listener {

        protected final Logger logger = LoggerFactory
                .getLogger(ServiceMonitorListener.class);
        protected final ServiceMonitor monitor;
        protected final Service service;

        public ServiceMonitorListener(ServiceMonitor monitor,
                Service service) {
            this.monitor = monitor;
            this.service = service;
        }

        protected void log(Service.State nextState,
                Optional<Service.State> prevState, Optional<Throwable> throwable) {
            checkArgument(nextState != null);
            String str = nextState.toString()
                    + (prevState.isPresent() ? String.format(" (%s)",
                            prevState.get()) : "")
                    + (throwable.isPresent() ? String.format(" %s",
                            throwable.get()) : "") + ": {}";
            if (nextState == Service.State.FAILED) {
                logger.warn(str, service);
            } else {
                logger.debug(str, service);
            }
        }

        protected void log(Service.State nextState) {
            this.log(nextState, Optional.<Service.State> absent(),
                    Optional.<Throwable> absent());
        }

        protected void log(Service.State nextState, Service.State prevState) {
            this.log(nextState, Optional.of(prevState),
                    Optional.<Throwable> absent());
        }

        @Override
        public void failed(State arg0, Throwable arg1) {
            log(Service.State.FAILED, Optional.of(arg0), Optional.of(arg1));
            if (service != monitor) {
                monitor.notifyChange();
            }
        }

        @Override
        public void running() {
            log(Service.State.RUNNING);
            if (service == monitor) {
                monitor.notifyChange();
            }
        }

        @Override
        public void starting() {
            log(Service.State.STARTING);
        }

        @Override
        public void stopping(State arg0) {
            log(Service.State.STOPPING, arg0);

        }

        @Override
        public void terminated(State arg0) {
            log(Service.State.TERMINATED, arg0);
            if (service != monitor) {
                monitor.notifyChange();
            }
        }

    }

    protected final Logger logger = LoggerFactory
            .getLogger(ServiceMonitor.class);
    protected Executor listenerExecutor;
    protected Executor executor;
    protected List<Service> services;

    @Inject
    protected ServiceMonitor(Executor executor) {
        this(executor, 
            MoreExecutors.sameThreadExecutor(),
            Collections.synchronizedList(Lists.<Service> newArrayList()));
    }
    
    protected ServiceMonitor(Executor executor, 
            Executor listenerExecutor,
            List<Service> services) {
        this.executor = executor;
        this.services = services;
        this.listenerExecutor = listenerExecutor;
    }

    @Override
    protected Executor executor() {
        return executor;
    }
    
    protected List<Service> services() {
        return services;
    }

    public boolean isAddable() {
        Service.State state = state();
        return (state == Service.State.NEW || state == Service.State.STARTING || state == Service.State.RUNNING);
    }

    public void add(Service service) {
        checkNotNull(service);
        checkState(isAddable(), state());
        services().add(service);
        listen(service);
        notifyChange();
    }

    public boolean remove(Service service) {
        if (services().remove(service)) {
            notifyChange();
            return true;
        }
        return false;
    }
    
    @Override
    protected void startUp() throws Exception {
        try {
            startTasks();
            listen(this);
        } catch (Exception e) {
            shutDown();
            throw e;
        }
    }

    @Override
    protected void shutDown() throws Exception {
        stopTasks();
    }

    protected void listen(Service service) {
        ServiceMonitorListener listener = new ServiceMonitorListener(this, service);
        service.addListener(listener, listenerExecutor);
    }

    protected void notifyChange() {
        if (isRunning()) {
            if (! monitorTasks()) {
                stop();
            }
        }
    }

    protected void startTasks() throws Exception {
        // start all currently monitored services
        // after this, services will be started by monitor()
        List<Service> services;
        synchronized (services()) {
            services = ImmutableList.copyOf(services());
        }
        //List<ListenableFuture<State>> futures = Lists.newArrayList();
        for (Service e : services) {
            State state = e.state();
            switch (state) {
            case NEW:
                //futures.add(e.start());
                // there may be dependencies between services
                // so don't start concurrently
                e.startAndWait();
                break;
            // it's possible that a service failed before we
            // started monitoring it
            case FAILED:
                throw new RuntimeException(e.failureCause());
            default:
                break;
            }
        }
        // wait for all to start
        //ListenableFuture<List<State>> allFutures = Futures.allAsList(futures);
        //allFutures.get();
    }

    protected void stopTasks() throws Exception {
        // first, notify all services to stop
        List<Service> services;
        synchronized (services()) {
            services = ImmutableList.copyOf(services());
        }
        List<ListenableFuture<State>> futures = Lists.newArrayList();
        for (Service e : services) {
            State state = e.state();
            switch (state) {
            case NEW:
            case STARTING:
            case RUNNING:
                futures.add(e.stop());
                break;
            default:
                break;
            }
        }
        
        // then, wait
        ListenableFuture<List<State>> allFutures = Futures.allAsList(futures);
        allFutures.get();

        // propagate error?
        for (Service e : services) {
            if (e.state() == State.FAILED) {
                throw new RuntimeException(e.failureCause());
            }
        }
    }

    protected boolean monitorTasks() {
        boolean stop = true;
        List<Service> services;
        synchronized (services()) {
            services = ImmutableList.copyOf(services());
        }
        for (Service e : services) {
            if (e == this) {
                continue;
            }
            State state = e.state();
            switch (state) {
            case NEW:
                e.start();
            case STARTING:
            case RUNNING:
                stop = stop && false;
                break;
            // stop everyone if someone failed
            case FAILED:
                stop = true;
                break;
            default:
                break;
            }
        }
        return !stop;
    }
}
