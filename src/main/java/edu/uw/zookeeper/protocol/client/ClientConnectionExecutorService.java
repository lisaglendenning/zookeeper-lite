package edu.uw.zookeeper.protocol.client;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;

/**
 * Wraps a lazily-instantiated ClientConnectionExecutor in a Service.
 */
public class ClientConnectionExecutorService<C extends Connection<? super Operation.Request>> extends AbstractIdleService 
        implements Reference<ClientConnectionExecutor<C>>, Publisher, ClientExecutor<Operation.Request, Message.ClientRequest<?>, Message.ServerResponse<?>> {

    public static <C extends Connection<? super Operation.Request>> ClientConnectionExecutorService<C> newInstance(
            Factory<ClientConnectionExecutor<C>> factory) {
        return new ClientConnectionExecutorService<C>(factory);
    }
    
    protected final Factory<ClientConnectionExecutor<C>> factory;
    protected volatile ClientConnectionExecutor<C> client;
    
    protected ClientConnectionExecutorService(
            Factory<ClientConnectionExecutor<C>> factory) {
        this.factory = factory;
        this.client = null;
    }
    
    protected Factory<ClientConnectionExecutor<C>> factory() {
        return factory;
    }
    
    @Override
    protected void startUp() throws Exception {
        assert (client == null);
        this.client = factory().get();
        client.register(this);
    }

    @Override
    protected void shutDown() throws Exception {
        if (client != null) {
            try {
                client.unregister(this);
            } catch (IllegalArgumentException e) {}
            try {
                client.submit(Records.Requests.getInstance().get(OpCode.CLOSE_SESSION)).get();
                client.get().close().get();
            } finally {
                client.stop();
            }
        }
    }

    @Override
    public ClientConnectionExecutor<C> get() {
        State state = state();
        switch (state) {
        case NEW:
        case STOPPING:
        case TERMINATED:
            throw new IllegalStateException(state.toString());
        default:
            break;
        }
        
        return client;
    }

    @Override
    public ListenableFuture<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> submit(Operation.Request request) {
        return get().submit(request);
    }

    @Override
    public ListenableFuture<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> submit(Operation.Request request, Promise<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> promise) {
        return get().submit(request, promise);
    }

    @Override
    public void post(Object object) {
        get().post(object);
    }

    @Override
    public void register(Object object) {
        get().register(object);
    }

    @Override
    public void unregister(Object object) {
        get().unregister(object);
    }

    @Subscribe
    public void handleStateEvent(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            stop();
        }
    }
}