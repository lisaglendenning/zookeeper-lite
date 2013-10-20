package edu.uw.zookeeper.net;

import net.engio.mbassy.PubSubSupport;

import com.google.common.util.concurrent.Service;

/**
 * Collection of Connections.
 * 
 * Posts at least the following events:
 * <ul>
 * <li> C when a new C is added
 * </ul>
 */
public interface ConnectionFactory<C extends Connection<?>> extends Iterable<C>, PubSubSupport<Object>, Service {
}
