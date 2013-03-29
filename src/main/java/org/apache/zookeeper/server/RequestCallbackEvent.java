package org.apache.zookeeper.server;

import org.apache.zookeeper.CallbackEvent;

public interface RequestCallbackEvent<T, V> extends CallbackEvent<V> {
    T request();
}
