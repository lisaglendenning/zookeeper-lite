package edu.uw.zookeeper.protocol.client;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.net.ConnectionStateEvent;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class ClientProtocolInitializer 
        extends ForwardingListenableFuture<Session> 
        implements Callable<ListenableFuture<Session>>, 
            ListenableFuture<Session>, 
            FutureCallback<OpCreateSession.Response> {

    public static ClientProtocolInitializer newInstance(
            ClientCodecConnection codecConnection,
            Factory<OpCreateSession.Request> factory) {
        return new ClientProtocolInitializer(
                codecConnection, factory, SettableFuturePromise.<Session>create());
    }

    private final Logger logger = LoggerFactory
            .getLogger(ClientProtocolInitializer.class);
    private final ClientCodecConnection codecConnection;
    private final Promise<Session> promise;
    private final Factory<OpCreateSession.Request> requests;

    private ClientProtocolInitializer(
            ClientCodecConnection codecConnection,
            Factory<OpCreateSession.Request> requests,
            Promise<Session> promise) {
        super();
        this.codecConnection = codecConnection;
        this.requests = requests;
        this.promise = promise;
    }
    
    @Override
    protected Promise<Session> delegate() {
        return promise;
    }
    
    @Override
    public ListenableFuture<Session> call() throws IOException {
        checkState(! isDone());
        checkState(codecConnection.asCodec().state() == ProtocolState.ANONYMOUS);
        OpCreateSession.Request message = requests.get();
        register();
        try {
            codecConnection.write(message);
        } catch (Exception e) {
            cancel(false);
            Throwables.propagateIfInstanceOf(e, IOException.class);
            throw Throwables.propagate(e);
        }
        return this;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (! isDone()) {
            unregister();
            return super.cancel(mayInterruptIfRunning);
        }
        return false;
    }

    @Subscribe
    public void handleConnectionState(ConnectionStateEvent event) {
        switch (event.event().to()) {
        case CONNECTION_CLOSED:
            onFailure(new KeeperException.ConnectionLossException());
            break;
        default:
            break;
        }
    }
    
    @Subscribe
    @Override
    public void onSuccess(OpCreateSession.Response result) {
        if (result instanceof OpCreateSession.Response.Valid) {
            if (! isDone()) {
                unregister();
                Session session = result.toSession();
                logger.info("Established Session: {}", session);
                delegate().set(session);
            }
        } else {
            onFailure(new KeeperException.SessionExpiredException());
        }
    }

    @Override
    public void onFailure(Throwable t) {
        if (! isDone()) {
            unregister();
            delegate().setException(t);
        }
    }

    private void register() {
        codecConnection.register(this);
        codecConnection.asConnection().register(this);
    }

    private void unregister() {
        try {
            codecConnection.unregister(this);
            codecConnection.asConnection().unregister(this);
        } catch (IllegalArgumentException e) {}
    } 
}
