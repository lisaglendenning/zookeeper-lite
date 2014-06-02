package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;

public class ServiceListenersService extends AbstractIdleService {
    
    public static ServiceListenersService forListeners(Iterable<? extends Service.Listener> listeners) {
        return new ServiceListenersService(listeners);
    }
    
    protected final Logger logger;
    protected final Listener delegate;
    
    protected ServiceListenersService(Iterable<? extends Service.Listener> listeners) {
        this.logger = LogManager.getLogger(this);
        this.delegate = new Listener(listeners);
    }
    
    @Override
    protected void startUp() throws Exception {
        try {
            delegate.starting();
            delegate.running();
        } catch (RuntimeException e) {
            delegate.stopping(State.STARTING);
            delegate.failed(State.STARTING, e);
            throw e;
        }
    }
    
    @Override
    protected void shutDown() throws Exception {
        try {
            delegate.stopping(State.RUNNING);
            delegate.terminated(State.STOPPING);
        } catch (RuntimeException e) {
            delegate.failed(State.STOPPING, e);
            throw e;
        }
    }
    
    protected class Listener extends Service.Listener implements Iterable<Service.Listener> {

        protected final ImmutableList<Service.Listener> listeners;
        protected final Map<Service.State, ImmutableList.Builder<Service.Listener>> called;

        public Listener(Iterable<? extends Service.Listener> listeners) {
            this.listeners = ImmutableList.copyOf(listeners);
            checkState(!this.listeners.isEmpty());
            this.called = Maps.newHashMap();
        }
        
        @Override
        public synchronized void starting() {
            if (called.containsKey(Service.State.STARTING)) {
                return;
            }
            ImmutableList.Builder<Service.Listener> builder = ImmutableList.builder();
            called.put(Service.State.STARTING, builder);
            for (Service.Listener listener: this) {
                listener.starting();
                builder.add(listener);
            }
        }
        
        @Override
        public synchronized void running() {
            if (!called.containsKey(Service.State.STARTING) || called.containsKey(Service.State.RUNNING)) {
                return;
            }
            ImmutableList.Builder<Service.Listener> builder = ImmutableList.builder();
            called.put(Service.State.RUNNING, builder);
            for (Service.Listener listener: called.get(Service.State.STARTING).build()) {
                listener.running();
                builder.add(listener);
            }
        }

        @Override
        public synchronized void stopping(State from) {
            if (!called.containsKey(Service.State.STARTING) || called.containsKey(Service.State.STOPPING)) {
                return;
            }
            ImmutableList.Builder<Service.Listener> builder = ImmutableList.builder();
            called.put(Service.State.STOPPING, builder);
            for (Service.Listener listener: Lists.reverse(called.get(Service.State.STARTING).build())) {
                try {
                    listener.stopping(from);
                } catch (Exception e) {
                    logger.warn("Error stopping {} in {}", listener, ServiceListenersService.this, e);
                }
                builder.add(listener);
            }
        }

        @Override
        public synchronized void terminated(State from) {
            if (!called.containsKey(Service.State.STOPPING) || called.containsKey(Service.State.TERMINATED) || called.containsKey(Service.State.FAILED)) {
                return;
            }
            ImmutableList.Builder<Service.Listener> builder = ImmutableList.builder();
            called.put(Service.State.TERMINATED, builder);
            for (Service.Listener listener: called.get(Service.State.STOPPING).build()) {
                try {
                    listener.terminated(from);
                } catch (Exception e) {
                    logger.warn("Error terminating {} in {}", listener, ServiceListenersService.this, e);
                }
                builder.add(listener);
            }
        }

        @Override
        public synchronized void failed(State from, Throwable failed) {
            if (!called.containsKey(Service.State.STOPPING) || called.containsKey(Service.State.TERMINATED) || called.containsKey(Service.State.FAILED)) {
                return;
            }
            ImmutableList.Builder<Service.Listener> builder = ImmutableList.builder();
            called.put(Service.State.TERMINATED, builder);
            for (Service.Listener listener: called.get(Service.State.STOPPING).build()) {
                try {
                    listener.failed(from, failed);
                } catch (Exception e) {
                    logger.warn("Error terminating {} in {}", listener, ServiceListenersService.this, e);
                }
                builder.add(listener);
            }
        }
        
        @Override
        public Iterator<Service.Listener> iterator() {
            return listeners.iterator();
        }
    }
}
