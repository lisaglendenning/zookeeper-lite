package edu.uw.zookeeper;

import edu.uw.zookeeper.util.Eventful;

/**
 * Collection of Connections.
 * 
 * Posts at least the following events:
 * <ul>
 * <li> Connection when a new Connection is added
 * </ul>
 */
public interface ConnectionGroup extends Iterable<Connection>, Eventful {
}
