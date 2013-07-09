package edu.uw.zookeeper.protocol.client;

import org.apache.zookeeper.KeeperException;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class ConnectTask
    extends PromiseTask<ConnectMessage.Request, Session> 
    implements FutureCallback<ConnectMessage.Request> {

    public static ConnectTask create(
            Connection<? super ConnectMessage.Request> connection,
            ConnectMessage.Request message) {
        return create(connection, message, SettableFuturePromise.<Session>create());
    }

    public static ConnectTask create(
            Connection<? super ConnectMessage.Request> connection,
            ConnectMessage.Request message, 
            Promise<Session> promise) {
        ConnectTask task = new ConnectTask(message, connection, promise);
        return task;
    }
    
    protected final Connection<? super ConnectMessage.Request> connection;
    
    protected ConnectTask(
            ConnectMessage.Request request,
            Connection<? super ConnectMessage.Request> connection,
            Promise<Session> promise) {
        super(request, promise);
        this.connection = connection;

        register();
        ListenableFuture<ConnectMessage.Request> future = null;
        try {
            future = connection.write(request);
        } catch (Throwable e) {
            onFailure(e);
        }
        if (future != null) {
            Futures.addCallback(future, this);
        }
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        unregister();
        if (! isDone()) {
            return super.cancel(mayInterruptIfRunning);
        }
        return false;
    }
    
    @Subscribe
    public void handleTransition(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            onFailure(new KeeperException.ConnectionLossException());
        }
    }
    
    @Subscribe
    public void handleConnectMessageResponse(ConnectMessage.Response result) {
        unregister();
        if (result instanceof ConnectMessage.Response.Valid) {
            if (! isDone()) {
                Session session = result.toSession();
                set(session);
            }
        } else {
            onFailure(new KeeperException.SessionExpiredException());
        }
    }
    
    @Override
    public void onSuccess(ConnectMessage.Request result) {
    }
    
    @Override
    public void onFailure(Throwable t) {
        unregister();
        if (! isDone()) {
            setException(t);
        }
    }
    
    protected void register() {
        connection.register(this);
    }
    
    protected void unregister() {
        try {
            connection.unregister(this);
        } catch (IllegalArgumentException e) {}
    } 
}
