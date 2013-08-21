package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.Records;

public class InitializeCache<T extends ZNodeViewCache<?,? super Records.Request,?>> extends AbstractIdleService implements Reference<T>{

    public static <T extends ZNodeViewCache<?,? super Records.Request,?>> InitializeCache<T> create(T cache) {
        return new InitializeCache<T>(cache);
    }
    
    protected final T cache;
    
    public InitializeCache(T cache) {
        this.cache = cache;
    }
    
    @Override
    public T get() {
        return cache;
    }

    @Override
    protected void startUp() throws Exception {
        TreeFetcher.builder().setClient(get()).build().apply(ZNodeLabel.Path.root()).get();
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
