package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;

/**
 * Application that runs a background Service.
 */
public class BackgroundServiceApplication implements Application {

    public static BackgroundServiceApplication newInstance(Runnable runnable, Service service) {
        return new BackgroundServiceApplication(runnable, service);
    }

    private final Logger logger = LoggerFactory
            .getLogger(BackgroundServiceApplication.class);
    private final Runnable runnable;
    private final Service service;

    @Inject
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
        service.stopAndWait();
    }
}
