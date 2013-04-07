package org.apache.zookeeper.util;

public interface Eventful {
    void post(Object event);

    void register(Object object);

    void unregister(Object object);
}
