package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;

/**
 * Application that starts a Service and waits for it to terminate.
 */
public class ServiceApplication implements Application {

    public static ServiceApplication forService(Service service) {
        return new ServiceApplication(service);
    }
    
    private final Logger logger;
    private final Service service;

    protected ServiceApplication(Service service) {
        this.logger = LogManager.getLogger(this);
        this.service = checkNotNull(service);
    }

    public Service service() {
        return service;
    }

    @Override
    public void run() {
        try {
            Services.start(service()).awaitTerminated();
        } catch (Exception e) {
            logger.error("{}", this, e);
            switch (service().state()) {
            case NEW:
            case STARTING:
            case RUNNING:
                Services.stopAndWait(service());
                break;
            default:
                break;
            }
            throw Throwables.propagate(e);
        } finally {
            switch (service().state()) {
            case FAILED:
                logger.error("FAILED: {}", this, service().failureCause());
                break;
            default:
                break;
            }
        }
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(service()).toString();
    }
}
