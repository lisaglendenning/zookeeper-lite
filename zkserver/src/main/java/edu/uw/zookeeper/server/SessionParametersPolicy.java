package edu.uw.zookeeper.server;

import edu.uw.zookeeper.common.TimeValue;

public interface SessionParametersPolicy {

    byte[] newPassword(long seed);

    boolean validatePassword(long seed, byte[] password);

    TimeValue boundTimeout(TimeValue timeOut);

    long newSessionId();

    TimeValue maxTimeout();

    TimeValue minTimeout();
}
