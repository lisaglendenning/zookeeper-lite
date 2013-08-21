package edu.uw.zookeeper.protocol.client;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

/**
 * Wraps a lazily-instantiated ClientConnectionExecutor in a Service.
 */
public class ClientConnectionExecutorService<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> extends AbstractIdleService 
        implements Reference<ClientConnectionExecutor<C>>, Publisher, ClientExecutor<Operation.Request, Message.ServerResponse<?>> {

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ClientConnectionExecutorService<C> newInstance(
            Factory<ListenableFuture<ClientConnectionExecutor<C>>> factory) {
        return new ClientConnectionExecutorService<C>(factory);
    }
    
    protected final Factory<ListenableFuture<ClientConnectionExecutor<C>>> factory;
    protected LinkedList<Object> handlers;
    protected LinkedList<Object> events;
    protected volatile ClientConnectionExecutor<C> client;
    
    protected ClientConnectionExecutorService(
            Factory<ListenableFuture<ClientConnectionExecutor<C>>> factory) {
        this.factory = factory;
        this.handlers = Lists.newLinkedList();
        this.events = Lists.newLinkedList();
        this.client = null;
    }

    @Override
    public ClientConnectionExecutor<C> get() {
        return client;
    }

    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(Operation.Request request) {
        if (! isRunning()) {
            throw new IllegalStateException();
        }
        
        return client.submit(request);
    }

    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(Operation.Request request, Promise<Message.ServerResponse<?>> promise) {
        if (! isRunning()) {
            throw new IllegalStateException();
        }

        return client.submit(request, promise);
    }

    @Override
    public synchronized void post(Object event) {
        if ((client == null) || !events.isEmpty()) {
            events.add(event);
        } else {
            client.post(event);
        }
    }

    @Override
    public synchronized void register(Object handler) {
        if ((client == null) || !handlers.isEmpty()) {
            handlers.add(handler);
        } else {
            client.register(handler);
        }
    }

    @Override
    public synchronized void unregister(Object handler) {
        if ((client == null) || !handlers.isEmpty()) {
            handlers.remove(handler);
        } else {
            client.unregister(handler);
        }
    }

    @Subscribe
    public void handleStateEvent(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            stop();
        }
    }

    @Override
    protected synchronized void startUp() throws Exception {
        assert (client == null);
        this.client = factory.get().get();
        client.session().get();
        
        Object handler;
        while ((handler = handlers.poll()) != null) {
            client.register(handler);
        }
        
        Object event;
        while ((event = events.poll()) != null) {
            client.post(event);
        }
        
        client.register(this);
        
        if (client.get().state().compareTo(Connection.State.CONNECTION_CLOSING) >= 0) {
            stop();
        }
    }

    @Override
    protected synchronized void shutDown() throws Exception {
        if (client != null) {
            try {
                client.unregister(this);
            } catch (IllegalArgumentException e) {}
            try {
                if ((client.get().codec().state() == ProtocolState.CONNECTED) && 
                        (client.get().state().compareTo(Connection.State.CONNECTION_CLOSING) < 0)) {
                    client.submit(Records.Requests.getInstance().get(OpCode.CLOSE_SESSION)).get(client.session().get().getTimeOut(), TimeUnit.MILLISECONDS);
                }
            } finally {
                client.stop();
            }
        }
        handlers.clear();
        events.clear();
    }
}
