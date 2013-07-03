package edu.uw.zookeeper.protocol.client;

import org.apache.zookeeper.KeeperException;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class ConnectTask
    extends PromiseTask<Connection<Message.ClientSessionMessage>, Session> 
    implements FutureCallback<ConnectMessage.Request> {

    public static ListenableFuture<Session> create(
            Connection<Message.ClientSessionMessage> connection,
            ConnectMessage.Request message) {
        return create(connection, message, SettableFuturePromise.<Session>create());
    }

    public static ListenableFuture<Session> create(
            Connection<Message.ClientSessionMessage> connection,
            ConnectMessage.Request message, 
            Promise<Session> promise) {
        ConnectTask task = new ConnectTask(connection, promise);
        ListenableFuture<ConnectMessage.Request> future = null;
        try {
            future = connection.write(message);
        } catch (Throwable e) {
            task.onFailure(e);
        }
        if (future != null) {
            Futures.addCallback(future, task);
        }
        return task;
    }
    
    protected ConnectTask(
            Connection<Message.ClientSessionMessage> connection,
            Promise<Session> promise) {
        super(connection, promise);
        register();
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
        task().register(this);
    }
    
    protected void unregister() {
        try {
            task().unregister(this);
        } catch (IllegalArgumentException e) {}
    } 
}
