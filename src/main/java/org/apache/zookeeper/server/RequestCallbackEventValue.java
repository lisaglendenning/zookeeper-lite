package org.apache.zookeeper.server;

import com.google.common.util.concurrent.FutureCallback;

public class RequestCallbackEventValue<T, V> implements RequestCallbackEvent<T, V> {
    
    protected T request;
    protected FutureCallback<V> callback;
    
    public RequestCallbackEventValue() {
        this(null, null);
    }

    public RequestCallbackEventValue(T request, FutureCallback<V> callback) {
        this.request = request;
        this.callback = callback;
    }
    
    @Override
    public T request() {
        return request;
    }
    
    public RequestCallbackEventValue<T, V> setRequest(T request) {
        this.request = request;
        return this;
    }

    @Override
    public FutureCallback<V> callback() {
        return callback;
    }

    public RequestCallbackEventValue<T, V> setCallback(FutureCallback<V> callback) {
        this.callback = callback;
        return this;
    }
}
