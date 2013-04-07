package org.apache.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;
import com.google.inject.Inject;

public class ApplicationService extends ExecutorServiceApplication {

    protected static class ApplicationServiceListener implements
            Service.Listener {

        protected ApplicationService service;

        public ApplicationServiceListener(ApplicationService service) {
            this.service = service;
        }

        @Override
        public void failed(State arg0, Throwable arg1) {
            service.complete();
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
            service.complete();
        }
    }

    protected final Service service;
    protected boolean completed;
    protected final Monitor monitor;
    protected final Monitor.Guard completedGuard;

    @Inject
    public ApplicationService(ExecutorService executor, Service service) {
        super(executor);
        this.service = checkNotNull(service, "service");
        this.completed = (service.state() == Service.State.FAILED || service
                .state() == Service.State.TERMINATED);
        this.monitor = new Monitor();
        this.completedGuard = new Monitor.Guard(monitor) {
            public boolean isSatisfied() {
                return completed;
            }
        };
        service.addListener(new ApplicationServiceListener(this),
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

    public void complete() {
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
