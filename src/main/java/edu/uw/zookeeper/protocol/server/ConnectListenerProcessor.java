package edu.uw.zookeeper.protocol.server;

import java.util.Map;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Publisher;

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
        ConnectMessage.Response response = delegate.apply(input.first());
        if (response instanceof ConnectMessage.Response.Valid) {
            Publisher prev = listeners.put(response.getSessionId(), input.second());
            if (prev != null) {
                // TODO
            }
        }
        input.second().post(response);
        return response;
    }
}
