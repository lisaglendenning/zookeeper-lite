package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
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

/**
 * Service that starts and monitors other Services.
 */
public class ServiceMonitor extends AbstractIdleService implements Iterable<Service> {

    public static ServiceMonitor create(Executor executor) {
        return new ServiceMonitor(executor);
    }

    /**
     * Logs Service state changes and notifies ServiceMonitor of significant changes.
     */
    private class ServiceMonitorListener implements Service.Listener {

        private final Logger logger = LoggerFactory
                .getLogger(ServiceMonitorListener.class);
        private final Service service;

        public ServiceMonitorListener(Service service) {
            this.service = service;
        }

        private void log(Service.State nextState,
                Optional<Service.State> prevState,
                Optional<Throwable> throwable) {
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

        private void log(Service.State nextState) {
            this.log(nextState, Optional.<Service.State> absent(),
                    Optional.<Throwable> absent());
        }

        private void log(Service.State nextState, Service.State prevState) {
            this.log(nextState, Optional.of(prevState),
                    Optional.<Throwable> absent());
        }

        @Override
        public void failed(State arg0, Throwable arg1) {
            log(Service.State.FAILED, Optional.of(arg0), Optional.of(arg1));
            if (service != ServiceMonitor.this) {
                notifyChange();
            }
        }

        @Override
        public void running() {
            log(Service.State.RUNNING);
            if (service == ServiceMonitor.this) {
                notifyChange();
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
            if (service != ServiceMonitor.this) {
                notifyChange();
            }
        }

    }

    private final Logger logger = LoggerFactory
            .getLogger(ServiceMonitor.class);
    private final Executor listenerExecutor;
    private final Executor executor;
    private final List<Service> services;

    @Inject
    protected ServiceMonitor(Executor executor) {
        this(executor, MoreExecutors.sameThreadExecutor(), Collections
                .synchronizedList(Lists.<Service> newArrayList()));
    }

    protected ServiceMonitor(Executor executor, Executor listenerExecutor,
            List<Service> services) {
        this.executor = executor;
        this.services = services;
        this.listenerExecutor = listenerExecutor;
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    public Iterator<Service> iterator() {
        return services.iterator();
    }

    public boolean isAddable() {
        Service.State state = state();
        return (state == Service.State.NEW || state == Service.State.STARTING || state == Service.State.RUNNING);
    }

    public void add(Service service) {
        checkNotNull(service);
        checkState(isAddable(), state());
        services.add(service);
        listen(service);
        notifyChange();
    }

    public boolean remove(Service service) {
        if (services.remove(service)) {
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

    private void listen(Service service) {
        ServiceMonitorListener listener = new ServiceMonitorListener(service);
        service.addListener(listener, listenerExecutor);
    }

    private void notifyChange() {
        if (isRunning()) {
            if (!monitorTasks()) {
                stop();
            }
        }
    }

    private void startTasks() throws Exception {
        // start all currently monitored services
        // after this, services will be started by monitor()
        List<Service> services;
        synchronized (this.services) {
            services = ImmutableList.copyOf(this.services);
        }
        // List<ListenableFuture<State>> futures = Lists.newArrayList();
        for (Service e : services) {
            State state = e.state();
            switch (state) {
            case NEW:
                // futures.add(e.start());
                // there may be dependencies between services
                // so don't start them concurrently
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
    }

    private void stopTasks() throws Exception {
        // first, notify all services to stop
        List<Service> services;
        synchronized (this.services) {
            services = ImmutableList.copyOf(this.services);
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

    private boolean monitorTasks() {
        List<Service> services;
        synchronized (this.services) {
            services = ImmutableList.copyOf(this.services);
        }
        boolean stop = true;
        for (Service e : services) {
            if (e == this) {
                continue;
            }
            State state = e.state();
            if (state == State.FAILED) {
                // stop all services if one service failed
                stop = true;
                break;
            } else if (state == State.NEW) {
                // start monitor services
                e.start();
                stop = false;
            } else if (state == State.STARTING || state == State.RUNNING) {
                // keep running as long as one service is running
                stop = false;
            }
        }
        return !stop;
    }
}
