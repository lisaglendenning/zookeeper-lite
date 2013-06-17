package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class ClientProtocolInitializer 
        extends PromiseTask<Factory<ConnectMessage.Request>, Session> 
        implements Callable<ListenableFuture<Session>>, 
            ListenableFuture<Session>, 
            FutureCallback<Message.ClientSessionMessage> {

    public static ClientProtocolInitializer newInstance(
            ClientCodecConnection codecConnection,
            Factory<ConnectMessage.Request> factory) {
        return new ClientProtocolInitializer(
                codecConnection, factory, SettableFuturePromise.<Session>create());
    }

    private final Logger logger = LoggerFactory
            .getLogger(ClientProtocolInitializer.class);
    private final Connection<Message.ClientSessionMessage> connection;
    private volatile ListenableFuture<Message.ClientSessionMessage> future;

    private ClientProtocolInitializer(
            Connection<Message.ClientSessionMessage> connection,
            Factory<ConnectMessage.Request> requests,
            Promise<Session> promise) {
        super(requests, promise);
        this.connection = connection;
        this.future = null;
        register();
    }
    
    @Override
    public synchronized ListenableFuture<Session> call() {
        if (! isDone() && future == null) {
            ConnectMessage.Request message = task().get();
            try {
                future = connection.write(message);
            } catch (Throwable e) {
                onFailure(e);
            }
            Futures.addCallback(future, this);
        }
        return this;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        unregister();
        if (! isDone()) {
            return super.cancel(mayInterruptIfRunning);
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Subscribe
    public void handleStateEvent(Automaton.Transition<?> event) {
        if (event.type().isAssignableFrom(Connection.State.class)) {
            handleConnectionStateEvent((Automaton.Transition<Connection.State>)event);
        }
    }
    
    public void handleConnectionStateEvent(Automaton.Transition<Connection.State> event) {
        switch (event.to()) {
        case CONNECTION_CLOSED:
            onFailure(new KeeperException.ConnectionLossException());
            break;
        default:
            break;
        }
    }

    @Subscribe
    public void handleCreateSessionResponse(ConnectMessage.Response result) {
        unregister();
        if (result instanceof ConnectMessage.Response.Valid) {
            if (! isDone()) {
                Session session = result.toSession();
                logger.info("Established Session: {}", session);
                set(session);
            }
        } else {
            onFailure(new KeeperException.SessionExpiredException());
        }
    }
    
    @Override
    public void onSuccess(Message.ClientSessionMessage result) {
    }

    @Override
    public void onFailure(Throwable t) {
        unregister();
        if (! isDone()) {
            setException(t);
        }
    }

    private void register() {
        connection.register(this);
    }

    private void unregister() {
        try {
            connection.unregister(this);
        } catch (IllegalArgumentException e) {}
    } 
}
