package edu.uw.zookeeper.protocol.client;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.SettableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.event.ConnectionStateEvent;
import edu.uw.zookeeper.protocol.OpCreateSession;

public class ClientConnectListener implements FutureCallback<OpCreateSession.Response> {

    private final Logger logger = LoggerFactory
            .getLogger(ClientConnectListener.class);
    private final ClientCodecConnection codecConnection;
    private final SettableFuture<Session> promise;
    
    public ClientConnectListener(ClientCodecConnection codecConnection) {
        this.promise = SettableFuture.create();
        this.codecConnection = codecConnection;
        register();
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
    
    public boolean cancel() {
        if (! promise.isDone()) {
            unregister();
            return promise.cancel(false);
        }
        return false;
    }
    
    public SettableFuture<Session> promise() {
        return promise;
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
            if (!promise.isDone()) {
                unregister();
                Session session = result.toSession();
                logger.info("Established Session: {}", session);
                promise.set(session);
            }
        } else {
            onFailure(new KeeperException.SessionExpiredException());
        }
    }

    @Override
    public void onFailure(Throwable t) {
        if (!promise.isDone()) {
            unregister();
            promise.setException(t);
        }
    }
}