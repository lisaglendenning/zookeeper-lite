package edu.uw.zookeeper.net;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.util.Eventful;

/**
 * Collection of Connections.
 * 
 * Posts at least the following events:
 * <ul>
 * <li> NewConnectionEvent when a new Connection is added
 * </ul>
 */
public interface ConnectionFactory extends Iterable<Connection>, Eventful, Service {
}