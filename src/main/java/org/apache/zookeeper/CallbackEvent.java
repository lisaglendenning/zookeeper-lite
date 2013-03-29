package org.apache.zookeeper;

import com.google.common.util.concurrent.FutureCallback;

public interface CallbackEvent<V> {
    FutureCallback<V> callback();
}
