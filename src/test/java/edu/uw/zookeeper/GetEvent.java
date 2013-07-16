package edu.uw.zookeeper;

import com.google.common.eventbus.Subscribe;
import com.google.common.reflect.TypeToken;

import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.ForwardingPromise;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class GetEvent<T> extends ForwardingPromise<T> {

    public static <T> GetEvent<T> newInstance(
            Eventful eventful) {
        Promise<T> delegate = SettableFuturePromise.create();
        return newInstance(eventful, delegate);
    }
    
    public static <T> GetEvent<T> newInstance(
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