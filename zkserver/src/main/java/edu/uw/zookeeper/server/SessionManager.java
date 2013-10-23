package edu.uw.zookeeper.server;

import net.engio.mbassy.PubSubSupport;
import edu.uw.zookeeper.protocol.Session;

public interface SessionManager extends PubSubSupport<SessionEvent>, Iterable<Session> {

    Session remove(long id);

    Session get(long id);

    Session validate(Session.Parameters parameters);

    Session validate(Session session);

    Session newSession();
}
