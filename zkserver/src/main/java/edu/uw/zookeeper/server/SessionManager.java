package edu.uw.zookeeper.server;

import edu.uw.zookeeper.protocol.Session;

public interface SessionManager extends Iterable<Session> {

    Session remove(long id);

    Session get(long id);

    Session validate(Session.Parameters parameters);

    Session validate(Session session);

    Session newSession();
}
