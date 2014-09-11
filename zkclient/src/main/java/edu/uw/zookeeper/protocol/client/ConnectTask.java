package edu.uw.zookeeper.protocol.client;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;

public class ConnectTask<C extends Connection<? super ConnectMessage.Request, ? extends Operation.Response,?>>
    extends PromiseTask<Pair<ConnectMessage.Request, C>, ConnectMessage.Response> 
    implements FutureCallback<ConnectMessage.Request>, Connection.Listener<Operation.Response>, Runnable {
    
    public static <C extends Connection<? super ConnectMessage.Request, ? extends Operation.Response,?>> ConnectTask<C> connect(
            ConnectMessage.Request message,
            C connection) {
        ConnectTask<C> task = create(message, connection, SettableFuturePromise.<ConnectMessage.Response>create());
        task.run();
        return task;
    }

    public static <C extends Connection<? super ConnectMessage.Request, ? extends Operation.Response,?>> ConnectTask<C> create(
            ConnectMessage.Request message, 
            C connection,
            Promise<ConnectMessage.Response> promise) {
        return new ConnectTask<C>(Pair.create(message, connection), promise);
    }
    
    protected Optional<ListenableFuture<ConnectMessage.Request>> write;
    
    protected ConnectTask(
            Pair<ConnectMessage.Request, C> task,
            Promise<ConnectMessage.Response> promise) {
        super(task, promise);
        this.write = Optional.absent();
        
        addListener(this, MoreExecutors.directExecutor());
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
        } else {
            setException(new IllegalStateException(String.valueOf(message)));
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
            if (! write.isPresent()) {
                task.second().subscribe(this);
                try {
                    write = Optional.of(task.second().write(task().first()));
                } catch (Throwable e) {
                    setException(e);
                    return;
                }
                Futures.addCallback(write.get(), this);
            }
        } else {
            task().second().unsubscribe(this);
            if (write.isPresent()) {
                write.get().cancel(true);
            }
        }
    } 
}
