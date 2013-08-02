package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

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
        return new ServiceMonitor(thisExecutor, listenerExecutor, stopOnTerminate, services);
    }


    protected final Logger logger;
    protected final Executor listenerExecutor;
    protected final Executor thisExecutor;
    protected final CopyOnWriteArrayList<Service> services;
    protected final boolean stopOnTerminate;

    protected ServiceMonitor() {
        this(MoreExecutors.sameThreadExecutor(),
                MoreExecutors.sameThreadExecutor(), 
                true, 
                ImmutableList.<Service>of());
    }
    
    protected ServiceMonitor(
            Executor thisExecutor, 
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
            logger.debug("Service added: {}", service);
            monitor(service);
            notifyChange();
            // TODO: check if still running
            return true;
        } else {
            return false;
        }
    }
    
    public boolean addOnStart(Service service) {
        checkNotNull(service);
        checkState(isAddable(), state());
        if (service.state() == Service.State.NEW) {
            service.addListener(new ServiceDelayedRegister(service), listenerExecutor);
            return true;
        } else {
            return add(service);
        }
    }

    public boolean remove(Service service) {
        boolean removed = services.remove(service);
        if (removed) {
            logger.debug("Service removed: {}", service);
            notifyChange();
        }
        return removed;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(Iterators.toString(iterator())).toString();
    }

    @Override
    protected Executor executor() {
        return thisExecutor;
    }

    @Override
    protected void startUp() throws ServiceException {
        logger.info("Starting: {}", this);
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
        logger.info("Stopping: {}", this);
        stopServices();
    }

    protected void monitor(Service service) {
        service.addListener(new ServiceMonitorListener(service), listenerExecutor);
    }

    protected void notifyChange() {
        if (isRunning()) {
            if (!monitorTasks()) {
                stop();
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
                logger.debug("Starting Service: {}", service);
                try {
                    service.start().get();
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
                    service.stop().get();
                } catch (Throwable t) {
                    logger.error("Error stopping Service: {}", service, t);
                    // only keep the first error?
                    if (cause == null) {
                        cause = new ServiceException(service, t);
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
                logger.debug("Service failed: {}", service, service.failureCause());
                stop = true;
                break;
            } else if (stopOnTerminate && (state == State.STOPPING || state == State.TERMINATED)) {
                // if stopOnTerminate policy is true, then stop the world
                // if one service stops
                logger.debug("Service stopped: {}", service);
                stop = true;
                break;
            } else if (state == State.NEW) {
                // start monitor services
                service.start();
                stop = false;
            } else if (state == State.STARTING || state == State.RUNNING) {
                // by default, keep running as long as one service is running
                stop = false;
            }
        }
        return !stop;
    }
    
    protected abstract class ServiceListener implements Service.Listener, Reference<Service> {

        private final Service service;
        
        public ServiceListener(Service service) {
            this.service = service;
        }

        @Override
        public Service get() {
            return service;
        }

        @Override
        public void starting() {
        }

        @Override
        public void running() {
        }

        @Override
        public void stopping(State from) {
        }

        @Override
        public void terminated(State from) {
        }

        @Override
        public void failed(State from, Throwable failure) {
        }
    }
    
    protected class ServiceDelayedRegister extends ServiceListener {

        public ServiceDelayedRegister(Service service) {
            super(service);
        }
        
        @Override
        public void starting() {
            ServiceMonitor.this.add(get());
        }
    }

    /**
     * Logs Service state changes and notifies ServiceMonitor of significant changes.
     */
    protected class ServiceMonitorListener extends ServiceListener {

        public ServiceMonitorListener(Service service) {
            super(service);
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
                logger.warn(str, get());
            } else {
                logger.debug(str, get());
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
        public void failed(State prevState, Throwable t) {
            log(Service.State.FAILED, Optional.of(prevState), Optional.of(t));
            if (get() != ServiceMonitor.this) {
                notifyChange();
            }
        }
    
        @Override
        public void running() {
            log(Service.State.RUNNING);
            if (get() == ServiceMonitor.this) {
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
    
        }
    
        @Override
        public void terminated(State prevState) {
            log(Service.State.TERMINATED, prevState);
            if (get() != ServiceMonitor.this) {
                notifyChange();
            }
        }
    }
}
