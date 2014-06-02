package edu.uw.zookeeper.client;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;

public class EnsembleViewFactory<T> implements DefaultsFactory<ServerInetAddressView, T> {

    public static <C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>> EnsembleViewFactory<ServerViewFactory<Session, OperationClientExecutor<C>>> fromSession(
            ClientConnectionFactory<C> connections,
            EnsembleView<ServerInetAddressView> view, 
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        return random(
                view,  
                ServerViewFactories.<C>newInstance(connections, timeOut, executor));
    }

    public static <T> EnsembleViewFactory<T> random(
            EnsembleView<ServerInetAddressView> view,
            ParameterizedFactory<ServerInetAddressView, T> factory) {
        return newInstance(
                view, 
                RandomSelector.<ServerInetAddressView>newInstance(),
                factory);
    }
    
    public static <T> EnsembleViewFactory<T> newInstance(
            EnsembleView<ServerInetAddressView> view,
            Function<ServerInetAddressView[], ServerInetAddressView> selector,
            ParameterizedFactory<ServerInetAddressView, T> factory) {
        return new EnsembleViewFactory<T>(
                view, selector, 
                InstanceFactory.newInstance(factory));
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
                ParameterizedFactory<V,T> factory) {
            return new InstanceFactory<V,T>(factory, 
                    Maps.<V,T>newHashMap());
        }
        
        protected final ParameterizedFactory<V, T> factory;
        protected final Map<V, T> instances;
        
        protected InstanceFactory(
                ParameterizedFactory<V, T> factory,
                Map<V, T> instances) {
            this.factory = factory;
            this.instances = instances;
        }

        @Override
        public synchronized T get(V value) {
            T instance = instances.get(value);
            if (instance == null) {
                instance = factory.get(value);
                instances.put(value, instance);
            }
            return instance;
        }
    }
    
    public static class ServerViewFactories<C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>> implements ParameterizedFactory<ServerInetAddressView, ServerViewFactory<Session, OperationClientExecutor<C>>> {

        public static <C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>> ServerViewFactories<C> newInstance(
                ClientConnectionFactory<C> connections,
                TimeValue timeOut,
                ScheduledExecutorService executor) {
            return new ServerViewFactories<C>(connections, timeOut, executor);
        }
        
        protected final ClientConnectionFactory<C> connections;
        protected final TimeValue timeOut;
        protected final ScheduledExecutorService executor;
        
        protected ServerViewFactories(
                ClientConnectionFactory<C> connections,
                TimeValue timeOut,
                ScheduledExecutorService executor) {
            this.connections = connections;
            this.timeOut = timeOut;
            this.executor = executor;
        }

        @Override
        public ServerViewFactory<Session, OperationClientExecutor<C>> get(ServerInetAddressView view) {
            return ServerViewFactory.defaults(connections, view, timeOut, executor);
        }
    }

    protected final Function<ServerInetAddressView[], ServerInetAddressView> selector;
    protected final EnsembleView<ServerInetAddressView> view;
    protected final ParameterizedFactory<ServerInetAddressView,T> factory;
    
    protected EnsembleViewFactory(
            EnsembleView<ServerInetAddressView> view,
            Function<ServerInetAddressView[], ServerInetAddressView> selector,
            ParameterizedFactory<ServerInetAddressView,T> factory) {
        this.view = view;
        this.selector = selector;
        this.factory = factory;
    }
    
    public EnsembleView<ServerInetAddressView> view() {
        return view;
    }
    
    public ServerInetAddressView select() {
        return selector.apply(Iterables.toArray(view, ServerInetAddressView.class));
    }

    @Override
    public T get() {
        return get(select());
    }
    
    @Override
    public T get(ServerInetAddressView server) {
        if (! view().contains(server)) {
            throw new IllegalArgumentException(server.toString());
        }
        return factory.get(server);
    }
}
