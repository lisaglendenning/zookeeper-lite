package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
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
    
    public static ServiceMonitor newInstance(Executor executor) {
        return new ServiceMonitor(Optional.of(executor));
    }
    
    public static ServiceMonitor newInstance(
            Optional<Executor> thisExecutor, 
            Executor listenerExecutor,
            boolean stopOnTerminate,
            List<Service> services) {
        return new ServiceMonitor(thisExecutor, listenerExecutor, stopOnTerminate, services);
    }


    protected final Logger logger;
    protected final Executor listenerExecutor;
    protected final Optional<Executor> thisExecutor;
    protected final CopyOnWriteArrayList<Service> services;
    protected volatile boolean stopOnTerminate;

    protected ServiceMonitor() {
        this(Optional.<Executor>absent());
    }

    protected ServiceMonitor(Optional<Executor> thisExecutor) {
        this(thisExecutor,
                MoreExecutors.sameThreadExecutor(), true, ImmutableList.<Service>of());
    }
    
    protected ServiceMonitor(
            Optional<Executor> thisExecutor, 
            Executor listenerExecutor,
            boolean stopOnTerminate,
            List<Service> services) {
        this.logger = LoggerFactory.getLogger(getClass());
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

    public void stopOnTerminate(boolean value) {
        this.stopOnTerminate = value;
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
        checkState(isAddable(), state());
        if (services.addIfAbsent(checkNotNull(service))) {
            logger.debug("Added Service: {}", service);
            monitor(service);
            notifyChange();
            return true;
        }
        return false;
    }
    
    public boolean addOnStart(Service service) {
        checkState(isAddable(), state());
        if (service.state() == Service.State.NEW) {
            service.addListener(new ServiceDelayedRegister(service), listenerExecutor);
            return true;
        } else {
            return add(service);
        }
    }

    public boolean remove(Service service) {
        if (services.remove(service)) {
            logger.debug("Removed Service: {}", service);
            notifyChange();
            return true;
        }
        return false;
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
        logger.info("Starting up");
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
        logger.info("Shutting down");
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
        for (Service service : services) {
            switch (service.state()) {
            case NEW:
                // there may be dependencies between services
                // so don't start them concurrently
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
        ServiceException cause = null;
        for (Service service : Lists.reverse(services)) {
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
                for (Service service : Lists.reverse(services)) {
                    if (service.state() == State.FAILED) {
                        throw new ServiceException(service, service.failureCause());
                    }
                }
            }
        }
    }

    protected boolean monitorTasks() {
        boolean stop = true; // stop if there are no services to monitor!
        for (Service service : services) {
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
        public void failed(State arg0, Throwable arg1) {
            log(Service.State.FAILED, Optional.of(arg0), Optional.of(arg1));
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
        public void stopping(State arg0) {
            log(Service.State.STOPPING, arg0);
    
        }
    
        @Override
        public void terminated(State arg0) {
            log(Service.State.TERMINATED, arg0);
            if (get() != ServiceMonitor.this) {
                notifyChange();
            }
        }
    }
}
