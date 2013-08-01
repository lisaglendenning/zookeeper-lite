package edu.uw.zookeeper.net;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.util.Publisher;

/**
 * Collection of Connections.
 * 
 * Posts at least the following events:
 * <ul>
 * <li> C when a new C is added
 * </ul>
 */
public interface ConnectionFactory<C extends Connection<?>> extends Iterable<C>, Publisher, Service {
}
