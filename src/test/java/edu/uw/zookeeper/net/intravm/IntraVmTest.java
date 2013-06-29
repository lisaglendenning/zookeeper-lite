package edu.uw.zookeeper.net.intravm;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.util.EventBusPublisher;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ForwardingPromise;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.SettableFuturePromise;

@RunWith(JUnit4.class)
public class IntraVmTest {

    public static class SameThreadSamePublisherEndpoints implements Factory<Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint>> {
        public final Reference<ListeningExecutorService> executor = Factories.Holder.of(MoreExecutors.sameThreadExecutor());
        public final Reference<EventBusPublisher> publisher = Factories.Holder.of(EventBusPublisher.newInstance());

        @Override
        public Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint> get() {
            return Pair.create(IntraVmConnectionEndpoint.create(publisher.get(), executor.get()),
                    IntraVmConnectionEndpoint.create(publisher.get(), executor.get()));
        }        
    }
    
    public static class SameConnection implements ParameterizedFactory<IntraVmConnection, IntraVmConnection> {
        @Override
        public IntraVmConnection get(IntraVmConnection value) {
            return value;
        }
    }
    
    public static class GetEvent extends ForwardingPromise<Object> {

        public static GetEvent newInstance(
                Eventful eventful) {
            Promise<Object> delegate = SettableFuturePromise.create();
            return newInstance(eventful, delegate);
        }
        
        public static GetEvent newInstance(
                Eventful eventful,
                Promise<Object> delegate) {
            return new GetEvent(eventful, delegate);
        }
        
        protected final Promise<Object> delegate;
        protected final Eventful eventful;
        
        public GetEvent(
                Eventful eventful,
                Promise<Object> delegate) {
            this.eventful = eventful;
            this.delegate = delegate;
            eventful.register(this);
        }
        
        @Subscribe
        public void handleEvent(Object event) {
            eventful.unregister(this);
            set(event);
        }

        @Override
        protected Promise<Object> delegate() {
            return delegate;
        }
    }
    
    @Test
    public void test() throws InterruptedException, ExecutionException {
        
        final SameThreadSamePublisherEndpoints endpointFactory = 
                new SameThreadSamePublisherEndpoints();
        final SameConnection connectionFactory = new SameConnection();
        IntraVmServerConnectionFactory<Object, IntraVmConnection> serverConnections = 
                IntraVmServerConnectionFactory.newInstance(
                endpointFactory.publisher.get(), endpointFactory, connectionFactory);
        IntraVmClientConnectionFactory<Object, IntraVmConnection> clientConnections = 
                IntraVmClientConnectionFactory.newInstance(
                endpointFactory.publisher.get(), connectionFactory);
        
        clientConnections.start().get();
        serverConnections.start().get();
        
        IntraVmConnection client = clientConnections.connect(serverConnections.listenAddress()).get();
        assertEquals(client, Iterables.getOnlyElement(clientConnections));
        IntraVmConnection server = Iterables.getOnlyElement(serverConnections);
        
        Object input = "hello";
        GetEvent listener = GetEvent.newInstance(server);
        client.write(input);
        Object output = listener.get();
        assertEquals(input, output);
        
        client.close();
        server.close();
        
        clientConnections.stop().get();
        serverConnections.stop().get();
        
    }
}
