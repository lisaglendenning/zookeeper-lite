package org.apache.zookeeper.server;

import java.util.concurrent.TimeUnit;

public interface SessionParametersPolicy {

    byte[] newPassword(long seed);
    
    boolean validatePassword(long seed, byte[] password);
    
    long boundTimeout(long timeOut, TimeUnit unit);
    
    long newSessionId();
    
    long maxTimeout();
    
    long minTimeout();

    TimeUnit timeoutUnit();
}
