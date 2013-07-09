package edu.uw.zookeeper.client;

import java.net.SocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.TimeValue;

public class EnsembleViewFactory<T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> implements DefaultsFactory<T, ClientConnectionExecutor<C>> {

    public static <T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> EnsembleViewFactory<T,C> newInstance(
            ClientConnectionFactory<?, C> connections,
            Class<T> type,
            EnsembleView<T> view, 
            TimeValue timeOut) {
        return new EnsembleViewFactory<T,C>(
                connections, view, timeOut, RandomSelector.newInstance(type));
    }
    
    public static class RandomSelector<T> implements Function<Iterable<T>, T> {
        
        public static <T> RandomSelector<T> newInstance(Class<T> type) {
            return new RandomSelector<T>(type);
        }
        
        protected final Random random;
        protected final Class<T> type;
        
        public RandomSelector(Class<T> type) {
            this.type = type;
            random = new Random();
        }
        
        @Override
        @Nullable
        public T apply(Iterable<T> input) {
            T[] array = Iterables.toArray(input, type);
            return (array.length == 0) ? null : array[random.nextInt(array.length)];
        }
    }

    protected final ClientConnectionFactory<?, C> connections;
    protected final Function<? super EnsembleView<T>, T> selector;
    protected final EnsembleView<T> view;
    protected final TimeValue timeOut;
    protected final ConcurrentMap<T, ServerViewFactory<T,C>> factories;
    
    protected EnsembleViewFactory(
            ClientConnectionFactory<?,C> connections,
            EnsembleView<T> view, 
            TimeValue timeOut,
            Function<? super EnsembleView<T>, T> selector) {
        this.view = view;
        this.timeOut = timeOut;
        this.selector = selector;
        this.connections = connections;
        this.factories = new ConcurrentHashMap<T, ServerViewFactory<T,C>>();
    }
    
    public EnsembleView<T> view() {
        return view;
    }
    
    @Override
    public ClientConnectionExecutor<C> get() {
        return get(selector.apply(view));
    }
    
    @Override
    public ClientConnectionExecutor<C> get(T server) {
        ServerViewFactory<T,C> factory = factories.get(server);
        if (factory == null) {
            factories.putIfAbsent(server, ServerViewFactory.newInstance(connections, server, timeOut));
            factory = factories.get(server);
        }
        return factory.get();
    }
}
