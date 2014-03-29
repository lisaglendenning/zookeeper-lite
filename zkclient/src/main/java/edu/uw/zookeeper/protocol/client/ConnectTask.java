package edu.uw.zookeeper.protocol.client;

import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;

public class ConnectTask
    extends PromiseTask<ConnectMessage.Request, ConnectMessage.Response> 
    implements FutureCallback<ConnectMessage.Request>, Connection.Listener<Operation.Response>, Runnable {
    
    public static ConnectTask connect(
            Connection<? super ConnectMessage.Request, ? extends Operation.Response,?> connection,
            ConnectMessage.Request message) {
        ConnectTask task = create(connection, message, SettableFuturePromise.<ConnectMessage.Response>create());
        task.run();
        return task;
    }

    public static ConnectTask create(
            Connection<? super ConnectMessage.Request, ? extends Operation.Response,?> connection,
            ConnectMessage.Request message, 
            Promise<ConnectMessage.Response> promise) {
        return new ConnectTask(message, connection, promise);
    }
    
    protected final Connection<? super ConnectMessage.Request, ? extends Operation.Response,?> connection;
    protected ListenableFuture<ConnectMessage.Request> future;
    
    protected ConnectTask(
            ConnectMessage.Request request,
            Connection<? super ConnectMessage.Request, ? extends Operation.Response,?> connection,
            Promise<ConnectMessage.Response> promise) {
        super(request, promise);
        this.connection = connection;
        this.future = null;
        
        addListener(this, SameThreadExecutor.getInstance());
    }

    @Override
    public void handleConnectionState(Automaton.Transition<Connection.State> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            onFailure(new KeeperException.ConnectionLossException());
        }
    }

    @Override
    public void handleConnectionRead(Operation.Response message) {
        if (message instanceof ConnectMessage.Response) {
            set((ConnectMessage.Response) message);
        }
    }
    
    @Override
    public void onSuccess(ConnectMessage.Request result) {
    }
    
    @Override
    public void onFailure(Throwable t) {
        setException(t);
    }
    
    @Override
    public synchronized void run() {
        if (! isDone()) {
            if (future == null) {
                connection.subscribe(this);
                try {
                    future = connection.write(task());
                } catch (Throwable e) {
                    setException(e);
                    return;
                }
                Futures.addCallback(future, this, SameThreadExecutor.getInstance());
            }
        } else {
            connection.unsubscribe(this);
            if (future != null) {
                future.cancel(true);
            }
        }
    } 
}
