package edu.uw.zookeeper.protocol.client;

import net.engio.mbassy.listener.Handler;

import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;

public class ConnectTask
    extends PromiseTask<ConnectMessage.Request, ConnectMessage.Response> 
    implements FutureCallback<ConnectMessage.Request> {

    public static ConnectTask create(
            Connection<? super ConnectMessage.Request> connection,
            ConnectMessage.Request message) {
        return create(connection, message, SettableFuturePromise.<ConnectMessage.Response>create());
    }

    public static ConnectTask create(
            Connection<? super ConnectMessage.Request> connection,
            ConnectMessage.Request message, 
            Promise<ConnectMessage.Response> promise) {
        ConnectTask task = new ConnectTask(message, connection, promise);
        return task;
    }
    
    protected final Connection<? super ConnectMessage.Request> connection;
    protected ListenableFuture<ConnectMessage.Request> future;
    
    protected ConnectTask(
            ConnectMessage.Request request,
            Connection<? super ConnectMessage.Request> connection,
            Promise<ConnectMessage.Response> promise) {
        super(request, promise);
        this.connection = connection;
        this.future = null;

        start();
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean cancel = super.cancel(mayInterruptIfRunning);
        if (cancel) {
            stop();
        }
        return cancel;
    }

    @Override
    public boolean set(ConnectMessage.Response result) {
        boolean set = super.set(result);
        if (set) {
            stop();
        }
        return set;
    }

    @Override
    public boolean setException(Throwable t) {
        boolean setException = super.setException(t);
        if (setException) {
            stop();
        }
        return setException;
    }
    
    @Handler
    public void handleTransition(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            setException(new KeeperException.ConnectionLossException());
        }
    }

    @Handler
    public void handleConnectMessageResponse(ConnectMessage.Response result) {
        set(result);
    }
    
    @Override
    public void onSuccess(ConnectMessage.Request result) {
    }
    
    @Override
    public void onFailure(Throwable t) {
        setException(t);
    }
    
    protected synchronized void start() {
        connection.subscribe(this);
        try {
            future = connection.write(task());
        } catch (Throwable e) {
            setException(e);
            return;
        }
        Futures.addCallback(future, this);
    }
    
    protected synchronized void stop() {
        try {
            connection.unsubscribe(this);
        } catch (IllegalArgumentException e) {}
        if (future != null) {
            future.cancel(true);
        }
    } 
}
