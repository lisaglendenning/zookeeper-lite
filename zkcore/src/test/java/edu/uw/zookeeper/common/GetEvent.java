package edu.uw.zookeeper.common;

import com.google.common.eventbus.Subscribe;
import com.google.common.reflect.TypeToken;

import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;

public class GetEvent<T> extends ForwardingPromise<T> {

    public static <T> GetEvent<T> create(
            Eventful eventful) {
        Promise<T> delegate = SettableFuturePromise.create();
        return create(eventful, delegate);
    }
    
    public static <T> GetEvent<T> create(
            Eventful eventful,
            Promise<T> delegate) {
        return new GetEvent<T>(eventful, delegate);
    }
    
    @SuppressWarnings("serial")
    protected final TypeToken<T> type = new TypeToken<T>(getClass()) {};
    protected final Promise<T> delegate;
    protected final Eventful eventful;
    
    public GetEvent(
            Eventful eventful,
            Promise<T> delegate) {
        this.eventful = eventful;
        this.delegate = delegate;
        eventful.register(this);
    }
    
    @Subscribe
    public void handleEvent(T event) {
        if (type.getRawType().isAssignableFrom(event.getClass())) {
            eventful.unregister(this);
            set(event);
        }
    }

    @Override
    protected Promise<T> delegate() {
        return delegate;
    }
}