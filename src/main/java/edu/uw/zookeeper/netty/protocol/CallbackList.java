package edu.uw.zookeeper.netty.protocol;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;

public class CallbackList<T> implements FutureCallback<T> {

    protected List<FutureCallback<T>> callbacks;

    public CallbackList(FutureCallback<T>... callbacks) {
        this.callbacks = Lists.newArrayList(callbacks);
    }

    @Override
    public void onSuccess(T result) {
        for (FutureCallback<T> callback : callbacks) {
            callback.onSuccess(result);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        for (FutureCallback<T> callback : callbacks) {
            callback.onFailure(t);
        }
    }

}
