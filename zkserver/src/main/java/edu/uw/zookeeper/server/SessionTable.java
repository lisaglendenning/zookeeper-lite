package edu.uw.zookeeper.server;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.Session.Parameters;
import edu.uw.zookeeper.common.Eventful;

public interface SessionTable extends Eventful, Iterable<Session> {

    Session remove(long id);

    Session get(long id);

    Session validate(Parameters parameters);

    Session validate(Session session);

    Session newSession();

}
