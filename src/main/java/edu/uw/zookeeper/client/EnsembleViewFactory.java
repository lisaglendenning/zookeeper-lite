package edu.uw.zookeeper.client;

import java.net.SocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;

public class EnsembleViewFactory<V extends ServerView.Address<? extends SocketAddress>, T> implements DefaultsFactory<V, T> {

    public static <V extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> EnsembleViewFactory<V, ServerViewFactory<Session, V, C>> newInstance(
            ClientConnectionFactory<C> connections,
            Class<V> type,
            EnsembleView<V> view, 
            TimeValue timeOut) {
        return newInstance(
                view, RandomSelector.<V>newInstance(), type, 
                InstanceFactory.newInstance(
                        ServerViewFactories.<V,C>newInstance(connections, timeOut), 
                        new MapMaker().<V,ServerViewFactory<Session, V, C>>makeMap()));
    }

    public static <V extends ServerView.Address<? extends SocketAddress>, T> EnsembleViewFactory<V,T> newInstance(
            EnsembleView<V> view,
            Function<V[], V> selector,
            Class<V> type,
            ParameterizedFactory<V,T> factory) {
        return new EnsembleViewFactory<V,T>(view, selector, type, factory);
    }
    
    public static class RandomSelector<T> implements Function<T[], T> {
        
        public static <T> RandomSelector<T> newInstance() {
            return new RandomSelector<T>();
        }
        
        protected final Random random;
        
        public RandomSelector() {
            random = new Random();
        }
        
        @Override
        @Nullable
        public T apply(T[] input) {
            return (input.length == 0) ? null : input[random.nextInt(input.length)];
        }
    }

    public static class InstanceFactory<V,T> implements ParameterizedFactory<V,T> {

        public static <V,T> InstanceFactory<V, T> newInstance(
                ParameterizedFactory<V,T> factory,
                ConcurrentMap<V,T> instances) {
            return new InstanceFactory<V,T>(factory, instances);
        }
        
        protected final ParameterizedFactory<V, T> factory;
        protected final ConcurrentMap<V, T> instances;
        
        protected InstanceFactory(
                ParameterizedFactory<V, T> factory,
                ConcurrentMap<V, T> instances) {
            this.factory = factory;
            this.instances = instances;
        }

        @Override
        public T get(V value) {
            T instance = instances.get(value);
            if (instance == null) {
                instances.putIfAbsent(value, factory.get(value));
                instance = instances.get(value);
            }
            return instance;
        }
    }
    
    public static class ServerViewFactories<V extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> implements ParameterizedFactory<V, ServerViewFactory<Session, V, C>> {

        public static <T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> ServerViewFactories<T,C> newInstance(
                ClientConnectionFactory<C> connections,
                TimeValue timeOut) {
            return new ServerViewFactories<T,C>(connections, timeOut);
        }
        
        protected final ClientConnectionFactory<C> connections;
        protected final TimeValue timeOut;
        
        protected ServerViewFactories(
                ClientConnectionFactory<C> connections,
                TimeValue timeOut) {
            this.connections = connections;
            this.timeOut = timeOut;
        }

        @Override
        public ServerViewFactory<Session, V, C> get(V view) {
            return ServerViewFactory.newInstance(connections, view, timeOut);
        }
    }

    protected final Function<V[], V> selector;
    protected final EnsembleView<V> view;
    protected final ParameterizedFactory<V,T> factory;
    protected final Class<V> type;
    
    protected EnsembleViewFactory(
            EnsembleView<V> view,
            Function<V[], V> selector,
            Class<V> type,
            ParameterizedFactory<V,T> factory) {
        this.view = view;
        this.selector = selector;
        this.factory = factory;
        this.type = type;
    }
    
    public EnsembleView<V> view() {
        return view;
    }
    
    public V select() {
        return selector.apply(Iterables.toArray(view, type));
    }

    @Override
    public T get() {
        return get(select());
    }
    
    @Override
    public T get(V server) {
        if (! view().contains(server)) {
            throw new IllegalArgumentException(server.toString());
        }
        return factory.get(server);
    }
}
