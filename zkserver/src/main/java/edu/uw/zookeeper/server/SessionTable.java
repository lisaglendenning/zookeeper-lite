package edu.uw.zookeeper.server;

import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.Session.Parameters;

public interface SessionTable extends Eventful, Iterable<Session> {

    Session remove(long id);

    Session get(long id);

    Session validate(Parameters parameters);

    Session validate(Session session);

    Session newSession();

}
