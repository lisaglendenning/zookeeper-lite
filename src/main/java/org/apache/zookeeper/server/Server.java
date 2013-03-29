package org.apache.zookeeper.server;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

public class Server extends AbstractIdleService  {

    protected RequestManager requests;
    protected SessionConnectionManager sessions;
    protected ServerConnectionGroup connections;
    
    @Inject
    protected Server(RequestManager requests, 
            SessionConnectionManager sessions,
            ServerConnectionGroup connections) {
        this.requests = requests;
        this.sessions = sessions;
        this.connections = connections;
    }
    
    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
    }
    
}
