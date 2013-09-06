package edu.uw.zookeeper.protocol.server;

import java.util.Map;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.protocol.ConnectMessage;

public class ConnectListenerProcessor implements Processor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> {

    public static ConnectListenerProcessor newInstance(
            Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> delegate,
            Map<Long, Publisher> listeners) {
        return new ConnectListenerProcessor(delegate, listeners);
    }
    
    protected final Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> delegate;
    protected final Map<Long, Publisher> listeners;
    
    public ConnectListenerProcessor(
            Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> delegate,
            Map<Long, Publisher> listeners) {
        this.delegate = delegate;
        this.listeners = listeners;
    }
    
    @Override
    public ConnectMessage.Response apply(Pair<ConnectMessage.Request, Publisher> input) throws Exception {
        ConnectMessage.Response output = delegate().apply(input.first());
        return apply(input, output);
    }
    
    protected ConnectMessage.Response apply(Pair<ConnectMessage.Request, Publisher> input, ConnectMessage.Response output) {
        Publisher publisher = input.second();
        if (output instanceof ConnectMessage.Response.Valid) {
            Publisher prev = listeners().put(output.getSessionId(), publisher);
            if (prev != null) {
                // TODO
            }
        }
        publisher.post(output);
        return output;
    }
    
    protected Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> delegate() {
        return delegate;
    }
    
    protected Map<Long, Publisher> listeners() {
        return listeners;
    }
}