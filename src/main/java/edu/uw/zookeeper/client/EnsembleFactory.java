package edu.uw.zookeeper.client;

import java.util.Collections;
import java.util.Map;
import java.util.Random;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerQuorumView;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientCodecConnection;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.TimeValue;

public class EnsembleFactory implements DefaultsFactory<ServerQuorumView, ClientProtocolConnection> {

    public static EnsembleFactory newInstance(
            ClientConnectionFactory connections,
            ParameterizedFactory<Connection, ? extends ClientCodecConnection> codecFactory,
            Processor<Operation.Request, Operation.SessionRequest> processor,
            EnsembleView view, TimeValue timeOut) {
        return new EnsembleFactory(connections, codecFactory, processor, view, timeOut, SelectServer.RANDOM);
    }
    
    public static enum SelectServer implements Function<EnsembleView, ServerQuorumView> {
        RANDOM {
            @Override
            @Nullable
            public ServerQuorumView apply(EnsembleView input) {
                Random random = new Random();
                ServerQuorumView[] servers = Iterables.toArray(input, ServerQuorumView.class);
                if (servers.length == 0) {
                    return null;
                }
                int index = random.nextInt(servers.length);
                return servers[index];
            }
        };
    }
    
    protected final ClientConnectionFactory connections;
    protected final ParameterizedFactory<Connection, ? extends ClientCodecConnection> codecFactory;
    protected final Function<EnsembleView, ServerQuorumView> selector;
    protected final EnsembleView view;
    protected final TimeValue timeOut;
    protected final Map<ServerQuorumView, ServerViewFactory> factories;
    protected final Processor<Operation.Request, Operation.SessionRequest> processor;
    
    protected EnsembleFactory(
            ClientConnectionFactory connections,
            ParameterizedFactory<Connection, ? extends ClientCodecConnection> codecFactory,
            Processor<Operation.Request, Operation.SessionRequest> processor,
            EnsembleView view, 
            TimeValue timeOut,
            Function<EnsembleView,ServerQuorumView> selector) {
        this.view = view;
        this.timeOut = timeOut;
        this.selector = selector;
        this.connections = connections;
        this.codecFactory = codecFactory;
        this.processor = processor;
        this.factories = Collections.synchronizedMap(Maps.<ServerQuorumView, ServerViewFactory>newHashMap());
    }
    
    @Override
    public ClientProtocolConnection get() {
        return get(selector.apply(view));
    }
    
    @Override
    public synchronized ClientProtocolConnection get(ServerQuorumView server) {
        ServerViewFactory factory = factories.get(server);
        if (factory == null) {
            factory = ServerViewFactory.newInstance(connections, codecFactory, processor, server, timeOut);
            factories.put(server, factory);
        }
        return factory.get();
    }
}
