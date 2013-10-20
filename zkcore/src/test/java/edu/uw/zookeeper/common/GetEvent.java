package edu.uw.zookeeper.common;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.listener.Handler;

import com.google.common.reflect.TypeToken;

import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;

public class GetEvent<T> extends ForwardingPromise<T> {

    public static <T> GetEvent<T> create(
            PubSubSupport<? super T> eventful) {
        Promise<T> delegate = SettableFuturePromise.create();
        return create(eventful, delegate);
    }
    
    public static <T> GetEvent<T> create(
            PubSubSupport<? super T> eventful,
            Promise<T> delegate) {
        return new GetEvent<T>(eventful, delegate);
    }
    
    @SuppressWarnings("serial")
    protected final TypeToken<T> type = new TypeToken<T>(getClass()) {};
    protected final Promise<T> delegate;
    protected final PubSubSupport<? super T> eventful;
    
    public GetEvent(
            PubSubSupport<? super T> eventful,
            Promise<T> delegate) {
        this.eventful = eventful;
        this.delegate = delegate;
        eventful.subscribe(this);
    }
    
    @Handler
    public void handleEvent(T event) {
        if (type.getRawType().isAssignableFrom(event.getClass())) {
            eventful.unsubscribe(this);
            set(event);
        }
    }

    @Override
    protected Promise<T> delegate() {
        return delegate;
    }
}