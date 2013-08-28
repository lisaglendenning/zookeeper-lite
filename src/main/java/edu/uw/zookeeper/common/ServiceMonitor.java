package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

/**
 * Service that starts and monitors other Services.
 */
public class ServiceMonitor extends AbstractIdleService implements Iterable<Service> {

    public static class ServiceException extends ExecutionException {
        private static final long serialVersionUID = -6599290449702356815L;
        
        /**
         * The Service that failed.
         */
        private final Service service;
        
        public ServiceException(Service service) {
            this(service, String.format("Service failed: %s", service));
        }

        public ServiceException(Service service, String message) {
            super(message);
            this.service = service;
        }

        public ServiceException(Service service, Throwable cause) {
            this(service,  String.format("Service failed: %s", service), cause);
        }

        public ServiceException(Service service, String message, Throwable cause) {
            super(message, cause);
            this.service = service;
        }
        
        public Service service() {
            return service;
        }
    }
    
    public static ServiceMonitor newInstance() {
        return new ServiceMonitor();
    }
    
    public static ServiceMonitor newInstance(
            Executor thisExecutor, 
            Executor listenerExecutor,
            boolean stopOnTerminate,
            Iterable<Service> services) {
        return new ServiceMonitor(Optional.of(thisExecutor), listenerExecutor, stopOnTerminate, services);
    }

    public static final Marker SERVICE_MONITOR_MARKER = MarkerManager.getMarker("EDU_UW_ZOOKEEPER_SERVICE_MONITOR");

    protected final Logger logger;
    protected final Executor listenerExecutor;
    protected final Optional<Executor> thisExecutor;
    protected final CopyOnWriteArrayList<Service> services;
    protected final boolean stopOnTerminate;

    protected ServiceMonitor() {
        this(Optional.<Executor>absent(),
                MoreExecutors.sameThreadExecutor(), 
                true, 
                ImmutableList.<Service>of());
    }
    
    protected ServiceMonitor(
            Optional<Executor> thisExecutor, 
            Executor listenerExecutor,
            boolean stopOnTerminate,
            Iterable<Service> services) {
        this.logger = LogManager.getLogger(getClass());
        this.stopOnTerminate = stopOnTerminate;
        this.thisExecutor = thisExecutor;
        this.services = Lists.newCopyOnWriteArrayList(services);
        this.listenerExecutor = listenerExecutor;
    }

    @Override
    public Iterator<Service> iterator() {
        return services.iterator();
    }

    public boolean stopOnTerminate() {
        return this.stopOnTerminate;
    }

    public boolean isAddable() {
        switch (state()) {
        case NEW:
        case STARTING:
        case RUNNING:
            return true;
        default:
            return false;
        }
    }

    public boolean add(Service service) {
        checkNotNull(service);
        checkState(isAddable(), state());
        if (services.addIfAbsent(service)) {
            monitor(service);
            notifyChange();
            // TODO: check if still running
            return true;
        } else {
            return false;
        }
    }
    
    public <T extends Service> T addOnStart(T service) {
        checkNotNull(service);
        checkState(isAddable(), state());
        if (! isMonitoring(service)) {
            if (service.state() == Service.State.NEW) {
                ServiceDelayedRegister listener = new ServiceDelayedRegister(service);
                service.addListener(listener, listenerExecutor);
                if (service.state() != Service.State.NEW) {
                    listener.run();
                }
            } else {
                add(service);
            }
        }
        return service;
    }
    
    public boolean isMonitoring(Service service) {
        return services.contains(service);
    }

    public boolean remove(Service service) {
        boolean removed = services.remove(service);
        if (removed) {
            notifyChange();
        }
        return removed;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(Iterators.toString(iterator())).toString();
    }

    @Override
    protected Executor executor() {
        if (thisExecutor.isPresent()) {
            return thisExecutor.get();
        } else {
            return super.executor();
        }
    }

    @Override
    protected void startUp() throws ServiceException {
        logger.debug(SERVICE_MONITOR_MARKER, "STARTING {}", this);
        try {
            startServices();
            monitor(this);
        } catch (ServiceException e) {
            shutDown();
            throw e;
        }
    }

    @Override
    protected void shutDown() throws ServiceException {
        logger.debug(SERVICE_MONITOR_MARKER, "STOPPING {}", this);
        stopServices();
    }

    protected void monitor(Service service) {
        service.addListener(new ServiceMonitorListener(service), listenerExecutor);
    }

    protected void notifyChange() {
        if (isRunning()) {
            if (!monitorTasks()) {
                stopAsync();
            }
        }
    }

    protected void startServices() throws ServiceException {
        // start all currently monitored services
        // after this, services will be started by monitor()
        // If any service fails during start up, fail everything
        for (Service service : this) {
            switch (service.state()) {
            case NEW:
                // there may be dependencies between services
                // so don't start them concurrently
                try {
                    service.startAsync();
                    service.awaitRunning();
                } catch (Throwable t) {
                    throw new ServiceException(service, t);
                }
                break;
            // it's possible that a service failed before we
            // started monitoring it
            case FAILED:
                throw new ServiceException(service, service.failureCause());
            default:
                break;
            }
        }
    }

    protected void stopServices() throws ServiceException {
        // stop all services in reverse order
        List<Service> reversed = Lists.reverse(services);
        ServiceException cause = null;
        for (Service service : reversed) {
            switch (service.state()) {
            case NEW:
            case STARTING:
            case RUNNING:
            case STOPPING:
                try {
                    service.stopAsync();
                    service.awaitTerminated();
                } catch (Throwable t) {
                    // only keep the first error?
                    if (cause == null) {
                        cause = new ServiceException(service, t);
                    } else {
                        logger.warn("Ignoring error {} for {}", t, service);
                    }
                }
                break;
            default:
                break;
            }
        }

        // if a service failed, set my state to an error state by propagating an error
        if (state() != State.FAILED) {
            if (cause != null) {
                throw cause;
            } else {
                for (Service service : reversed) {
                    if (service.state() == State.FAILED) {
                        throw new ServiceException(service, service.failureCause());
                    }
                }
            }
        }
    }

    protected boolean monitorTasks() {
        boolean stop = true; // stop if there are no services to monitor!
        for (Service service : this) {
            if (service == this) {
                continue;
            }
            State state = service.state();
            if (state == State.FAILED) {
                // stop all services if one service failed
                stop = true;
                break;
            } else if (stopOnTerminate && (state == State.STOPPING || state == State.TERMINATED)) {
                // if stopOnTerminate policy is true, then stop the world
                // if one service stops
                stop = true;
                break;
            } else if (state == State.NEW) {
                // start monitored services
                service.startAsync();
                stop = false;
            } else if (state == State.STARTING || state == State.RUNNING) {
                // by default, keep running as long as one service is running
                stop = false;
            }
        }
        return !stop;
    }
    
    protected abstract class ServiceListenerAdapter extends Service.Listener {

        protected final Service service;
        
        public ServiceListenerAdapter(Service service) {
            this.service = service;
        }
    }
    
    protected class ServiceDelayedRegister extends ServiceListenerAdapter implements Runnable {

        public ServiceDelayedRegister(Service service) {
            super(service);
        }
        
        @Override
        public void starting() {
            run();
        }

        @Override
        public void run() {
            if (! isMonitoring(service)) {
                try {
                    add(service);
                } catch (IllegalStateException e) {
                    service.stopAsync();
                }
            }
        }
    }

    /**
     * Logs Service state changes and notifies ServiceMonitor of significant changes.
     */
    protected class ServiceMonitorListener extends ServiceListenerAdapter {

        public ServiceMonitorListener(Service service) {
            super(service);
            
            // log current state
            Service.State state = service.state();
            switch (state) {
            case NEW:
                break;
            case STARTING:
            case RUNNING:
            case STOPPING:
            case TERMINATED:
                log(state);
                break;
            case FAILED:
                log(state, Optional.<Service.State>absent(), Optional.of(service.failureCause()));
                break;
            }
        }
    
        @Override
        public void failed(State prevState, Throwable t) {
            log(Service.State.FAILED, Optional.of(prevState), Optional.of(t));
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
        public void stopping(State prevState) {
            log(Service.State.STOPPING, prevState);
            if (service != ServiceMonitor.this) {
                notifyChange();
            }
        }
    
        @Override
        public void terminated(State prevState) {
            log(Service.State.TERMINATED, prevState);
            if (service != ServiceMonitor.this) {
                notifyChange();
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

        private void log(Service.State nextState,
                Optional<Service.State> prevState,
                Optional<Throwable> throwable) {
            if (logger.isWarnEnabled()) {
                String str = String.format("[%s]", nextState.toString())
                        + (prevState.isPresent() ? String.format(" ([%s])",
                                prevState.get()) : "")
                        + (throwable.isPresent() ? String.format(" [%s]",
                                throwable.get()) : "") + ": {}";
                if (nextState == Service.State.FAILED) {
                    logger.warn(SERVICE_MONITOR_MARKER, str, service);
                } else {
                    logger.debug(SERVICE_MONITOR_MARKER, str, service);
                }
            }
        }
    }
}
