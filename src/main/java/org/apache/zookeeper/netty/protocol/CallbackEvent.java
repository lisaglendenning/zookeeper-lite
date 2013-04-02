package org.apache.zookeeper.netty.protocol;

import com.google.common.util.concurrent.FutureCallback;

public class CallbackEvent {

    protected FutureCallback<Void> callback = null;

    public CallbackEvent setCallback(FutureCallback<Void> callback) {
        this.callback = callback;
        return this;
    }
    
    public FutureCallback<Void> getCallback() {
        return callback;
    }

}
