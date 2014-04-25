package edu.uw.zookeeper.server;

import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Session;

public interface SessionManager extends Iterable<Session> {

    Session remove(long id);

    Session get(long id);
    
    Session put(Session session);

    Session create(TimeValue timeOut);
    
    Session create();
}
