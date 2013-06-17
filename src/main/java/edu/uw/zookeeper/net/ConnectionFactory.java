package edu.uw.zookeeper.net;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.util.Eventful;

/**
 * Collection of Connections.
 * 
 * Posts at least the following events:
 * <ul>
 * <li> C when a new C is added
 * </ul>
 */
public interface ConnectionFactory<I, C extends Connection<I>> extends Iterable<C>, Eventful, Service {
}
