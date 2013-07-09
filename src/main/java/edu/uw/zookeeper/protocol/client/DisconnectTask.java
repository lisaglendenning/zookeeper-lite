package edu.uw.zookeeper.protocol.client;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.proto.IDisconnectRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.ForwardingPromise;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class DisconnectTask<V> extends ForwardingPromise<V> implements Runnable {

    public static <V> DisconnectTask<V> create(V value, Connection<? super IDisconnectRequest> connection) {
        return new DisconnectTask<V>(value, connection, SettableFuturePromise.<V>create());
    }
    
    protected final V value;
    protected final Connection<? super IDisconnectRequest> connection;
    protected final Promise<V> delegate;
    protected volatile ListenableFuture<IDisconnectRequest> writeFuture;
    protected volatile ListenableFuture<?> closeFuture;
    
    public DisconnectTask(
            V value,
            Connection<? super IDisconnectRequest> connection,
            Promise<V> promise) {
        this.value = value;
        this.connection = connection;
        this.delegate = promise;
        this.writeFuture = null;
        this.closeFuture = null;
    }
    
    @Override
    public void run() {
        if (isDone()) {
            return;
        }
        
        synchronized (this) {
            if (writeFuture == null) {
                IDisconnectRequest request = Records.newInstance(IDisconnectRequest.class);
                writeFuture = connection.write(request);
            }
        }
        
        if (writeFuture.isDone()) {
            synchronized (this) {
                if (closeFuture == null) {
                    closeFuture = connection.close();
                }
            }
            if (closeFuture.isDone()) {
                try {
                    closeFuture.get();
                    set(value);
                } catch (Throwable t) {
                    setException(t);
                }
            } else {
                closeFuture.addListener(this, connection);
                return;
            }
        } else {
            writeFuture.addListener(this, connection);
            return;
        }
    }

    @Override
    protected Promise<V> delegate() {
        return delegate;
    }
}