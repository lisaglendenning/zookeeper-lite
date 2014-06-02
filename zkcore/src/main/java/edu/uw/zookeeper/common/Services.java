package edu.uw.zookeeper.common;

import com.google.common.util.concurrent.Service;

public abstract class Services {

    public static Service start(Service service) {
        switch (service.state()) {
        case NEW:
            service.startAsync();
            break;
        default:
            break;
        }
        return service;
    }
    
    public static Service stop(Service service) {
        switch (service.state()) {
        case NEW:
        case STARTING:
        case RUNNING:
            service.stopAsync();
        default:
            break;
        }
        return service;
    }
    
    public static Service startAndWait(Service service) {
        start(service).awaitRunning();
        return service;
    }
    
    public static Service stopAndWait(Service service) {
        stop(service).awaitTerminated();
        return service;
    }
}
