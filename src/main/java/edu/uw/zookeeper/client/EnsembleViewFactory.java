package edu.uw.zookeeper.client;

import java.util.Collections;
import java.util.Map;
import java.util.Random;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientCodecConnection;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.TimeValue;

public class EnsembleViewFactory implements DefaultsFactory<ServerView.Address<?>, ClientProtocolExecutor> {

    public static <C extends ClientCodecConnection> EnsembleViewFactory newInstance(
            ClientConnectionFactory<Message.ClientSessionMessage, C> connections,
            Processor<Operation.Request, Operation.SessionRequest> processor,
            EnsembleView<? extends ServerView.Address<?>> view, 
            TimeValue timeOut) {
        return new EnsembleViewFactory(connections, processor, view, timeOut, SelectServer.RANDOM);
    }
    
    public static enum SelectServer implements Function<EnsembleView<? extends ServerView.Address<?>>, ServerView.Address<?>> {
        RANDOM {
            @Override
            @Nullable
            public ServerView.Address<?> apply(EnsembleView<? extends ServerView.Address<?>> input) {
                Random random = new Random();
                ServerView.Address<?>[] servers = Iterables.toArray(input, ServerView.Address.class);
                if (servers.length == 0) {
                    return null;
                }
                int index = random.nextInt(servers.length);
                return servers[index];
            }
        };
    }
    
    protected final ClientConnectionFactory<Message.ClientSessionMessage, ? extends ClientCodecConnection> connections;
    protected final Function<EnsembleView<? extends ServerView.Address<?>>, ServerView.Address<?>> selector;
    protected final EnsembleView<? extends ServerView.Address<?>> view;
    protected final TimeValue timeOut;
    protected final Map<ServerView.Address<?>, ServerViewFactory> factories;
    protected final Processor<Operation.Request, Operation.SessionRequest> processor;
    
    protected EnsembleViewFactory(
            ClientConnectionFactory<Message.ClientSessionMessage, ? extends ClientCodecConnection> connections,
            Processor<Operation.Request, Operation.SessionRequest> processor,
            EnsembleView<? extends ServerView.Address<?>> view, 
            TimeValue timeOut,
            Function<EnsembleView<? extends ServerView.Address<?>>, ServerView.Address<?>> selector) {
        this.view = view;
        this.timeOut = timeOut;
        this.selector = selector;
        this.connections = connections;
        this.processor = processor;
        this.factories = Collections.synchronizedMap(Maps.<ServerView.Address<?>, ServerViewFactory>newHashMap());
    }
    
    public EnsembleView<? extends ServerView.Address<?>> view() {
        return view;
    }
    
    @Override
    public ClientProtocolExecutor get() {
        return get(selector.apply(view));
    }
    
    @Override
    public synchronized ClientProtocolExecutor get(ServerView.Address<?> server) {
        ServerViewFactory factory = factories.get(server);
        if (factory == null) {
            factory = ServerViewFactory.newInstance(connections, processor, server, timeOut);
            factories.put(server, factory);
        }
        return factory.get();
    }
}
