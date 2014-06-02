package edu.uw.zookeeper.common;

import java.util.Iterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;

public final class ForwardingServicesListener<T extends Service> extends Service.Listener implements Iterable<T> {

    public static <T extends Service> ForwardingServicesListener<T> forServices(Iterable<T> services) {
        return new ForwardingServicesListener<T>(ImmutableList.copyOf(services));
    }
    
    protected final ImmutableList<T> delegate;
    
    protected ForwardingServicesListener(ImmutableList<T> delegate) {
        this.delegate = delegate;
    }
    
    @Override
    public void starting() {
        for (Service e: this) {
            Services.startAndWait(e);
        }
    }

    @Override
    public void terminated(Service.State from) {
        for (Service e: Lists.reverse(delegate)) {
            Services.stopAndWait(e);
        }
    }

    @Override
    public Iterator<T> iterator() {
        return delegate.iterator();
    }
}
