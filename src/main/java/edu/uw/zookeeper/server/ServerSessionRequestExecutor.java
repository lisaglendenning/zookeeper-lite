package edu.uw.zookeeper.server;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.protocol.Operation;

public class ServerSessionRequestExecutor implements SessionRequestExecutor {

    protected ServerSessionRequestExecutor() {
    }

    @Override
    public ListenableFuture<Operation.SessionReply> submit(Operation.SessionRequest request) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void register(Object object) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void unregister(Object object) {
        // TODO Auto-generated method stub
        
    }
}
