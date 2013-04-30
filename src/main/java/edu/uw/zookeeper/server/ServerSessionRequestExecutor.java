package edu.uw.zookeeper.server;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.protocol.Operation;

public class ServerSessionRequestExecutor implements SessionRequestExecutor {

    public static ServerSessionRequestExecutor newInstance(long sessionId) {
        return new ServerSessionRequestExecutor(sessionId);
    }
    
    protected final long sessionId;
    
    protected ServerSessionRequestExecutor(long sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public ListenableFuture<Operation.SessionReply> submit(Operation.SessionRequest request) {
        System.out.printf("0x%s: %s%n", Long.toHexString(sessionId), request);
        return SettableFuture.create();
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
