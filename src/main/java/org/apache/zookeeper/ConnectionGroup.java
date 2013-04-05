package org.apache.zookeeper;

import org.apache.zookeeper.util.Eventful;

public interface ConnectionGroup extends Iterable<Connection>, Eventful {
}
