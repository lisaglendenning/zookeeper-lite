package edu.uw.zookeeper.protocol.server;

import java.util.Map;

import net.engio.mbassy.PubSubSupport;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.protocol.ConnectMessage;

public class ConnectListenerProcessor implements Processor<Pair<ConnectMessage.Request, PubSubSupport<Object>>, ConnectMessage.Response> {

    public static ConnectListenerProcessor newInstance(
            Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> delegate,
            Map<Long, PubSubSupport<Object>> listeners) {
        return new ConnectListenerProcessor(delegate, listeners);
    }
    
    protected final Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> delegate;
    protected final Map<Long, PubSubSupport<Object>> listeners;
    
    public ConnectListenerProcessor(
            Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> delegate,
            Map<Long, PubSubSupport<Object>> listeners) {
        this.delegate = delegate;
        this.listeners = listeners;
    }
    
    @Override
    public ConnectMessage.Response apply(Pair<ConnectMessage.Request, PubSubSupport<Object>> input) throws Exception {
        ConnectMessage.Response output = delegate.apply(input.first());
        return apply(input, output);
    }
    
    protected ConnectMessage.Response apply(Pair<ConnectMessage.Request, PubSubSupport<Object>> input, ConnectMessage.Response output) {
        PubSubSupport<Object> publisher = input.second();
        if (output instanceof ConnectMessage.Response.Valid) {
            PubSubSupport<Object> prev = listeners.put(output.getSessionId(), publisher);
            if (prev != null) {
                // TODO
            }
        }
        publisher.publish(output);
        return output;
    }
}
