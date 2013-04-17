package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;
import com.google.inject.Inject;

/**
 * Application that starts a Service and waits for it to terminate.
 */
public class ApplicationService extends ExecutorServiceApplication {

    private class ApplicationServiceListener implements
            Service.Listener {

        @Override
        public void failed(State arg0, Throwable arg1) {
            complete();
        }

        @Override
        public void running() {
        }

        @Override
        public void starting() {
        }

        @Override
        public void stopping(State arg0) {
        }

        @Override
        public void terminated(State arg0) {
            complete();
        }
    }

    private final Service service;
    private boolean completed;
    private final Monitor monitor;
    private final Monitor.Guard completedGuard;

    @Inject
    public ApplicationService(ExecutorService executor, Service service) {
        super(executor);
        this.service = checkNotNull(service);
        this.completed = (service.state() == Service.State.FAILED || service
                .state() == Service.State.TERMINATED);
        this.monitor = new Monitor();
        this.completedGuard = new Monitor.Guard(monitor) {
            public boolean isSatisfied() {
                return completed;
            }
        };
        service.addListener(new ApplicationServiceListener(),
                MoreExecutors.sameThreadExecutor());
    }

    public Service service() {
        return service;
    }

    public boolean completed() {
        monitor.enter();
        try {
            return completed;
        } finally {
            monitor.leave();
        }
    }

    private void complete() {
        monitor.enter();
        try {
            completed = true;
        } finally {
            monitor.leave();
        }
    }

    @Override
    public Void call() throws Exception {
        service().start();
        monitor.enter();
        try {
            monitor.waitFor(completedGuard);
        } finally {
            monitor.leave();
        }

        // propagate errors
        if (service.state() == State.FAILED) {
            throw new RuntimeException(service().failureCause());
        }

        return super.call();
    }
}