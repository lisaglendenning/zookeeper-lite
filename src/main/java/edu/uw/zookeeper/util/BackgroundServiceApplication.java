package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import com.google.common.util.concurrent.Service;

/**
 * Application that runs a background Service.
 */
public class BackgroundServiceApplication implements Application {

    public static BackgroundServiceApplication newInstance(Runnable runnable, Service service) {
        return new BackgroundServiceApplication(runnable, service);
    }

    private final Runnable runnable;
    private final Service service;

    public BackgroundServiceApplication(Runnable runnable, Service service) {
        this.runnable = checkNotNull(runnable);
        this.service = checkNotNull(service);
    }
    
    public Runnable runnable() {
        return runnable;
    }

    public Service service() {
        return service;
    }

    @Override
    public void run() {
        service().startAndWait();
        runnable().run();
        service().stopAndWait();
    }
}
