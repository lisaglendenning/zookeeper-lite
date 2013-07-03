package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class ClientProtocolInitializer 
        extends PromiseTask<Factory<ConnectMessage.Request>, Session> 
        implements Callable<ListenableFuture<Session>> {

    public static ClientProtocolInitializer newInstance(
            ClientCodecConnection codecConnection,
            Factory<ConnectMessage.Request> factory) {
        return new ClientProtocolInitializer(
                codecConnection, factory, SettableFuturePromise.<Session>create());
    }

    private final Logger logger = LoggerFactory
            .getLogger(ClientProtocolInitializer.class);
    private final Connection<Message.ClientSessionMessage> connection;
    private volatile ListenableFuture<Session> future;

    private ClientProtocolInitializer(
            Connection<Message.ClientSessionMessage> connection,
            Factory<ConnectMessage.Request> requests,
            Promise<Session> promise) {
        super(requests, promise);
        this.connection = connection;
        this.future = null;
    }
    
    @Override
    public synchronized ListenableFuture<Session> call() {
        if (! isDone() && future == null) {
            ConnectMessage.Request message = task().get();
            future = ConnectTask.create(connection, message, this);
        }
        return this;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (! isDone()) {
            if (future != null) {
                return future.cancel(mayInterruptIfRunning);
            } else {
                return super.cancel(mayInterruptIfRunning);
            }
        }
        return false;
    }

    @Override
    public boolean set(Session session) {
        boolean isSet = super.set(session);
        if (isSet) {
            logger.info("Established Session: {}", session);
        }
        return isSet;
    }
}
