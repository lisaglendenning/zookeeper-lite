package edu.uw.zookeeper.protocol.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.ServerExecutor;
import edu.uw.zookeeper.event.ConnectionStateEvent;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Stateful;

public class ServerProtocolConnection extends ForwardingEventful implements FutureCallback<Message.ServerMessage>, Stateful<ProtocolState> {

    public static ServerProtocolConnection create(
            Publisher publisher,
            ServerExecutor executor,
            ServerCodecConnection codecConnection) {
        return new ServerProtocolConnection(publisher, executor, codecConnection);
    }

    protected final Logger logger = LoggerFactory
            .getLogger(ServerProtocolConnection.class);
    private final ServerExecutor executor;
    private final ServerCodecConnection codecConnection;
    
    private ServerProtocolConnection(
            Publisher publisher,
            ServerExecutor executor,
            ServerCodecConnection codecConnection) {
        super(publisher);
        this.executor = executor;
        this.codecConnection = codecConnection;
        register(this);
    }

    @Override
    public ProtocolState state() {
        return codecConnection.asCodec().state();
    }

    @Subscribe
    public void handleConnectionState(ConnectionStateEvent event) {
        switch (event.event().to()) {
        case CONNECTION_CLOSED:
            unregister(this);
            break;
        default:
            break;
        }
    }
    
    @Subscribe
    public void handleMessage(Message.ClientMessage message) {
        ListenableFuture<Message.ServerMessage> future = executor.submit(message);
        Futures.addCallback(future, this);
    }

    /**
     * Notifications end up here somehow
     */
    @Subscribe
    @Override
    public void onSuccess(Message.ServerMessage result) {
        try {
            codecConnection.write(result);
        } catch (Exception e) {
            logger.warn("Dropping {}", result, e);
            onFailure(e);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        logger.warn("Closing connection", t);
        codecConnection.asConnection().close();
    }

    @Override
    public void register(Object handler) {
        codecConnection.register(handler);
        codecConnection.asConnection().register(handler);
        super.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        codecConnection.unregister(handler);
        codecConnection.asConnection().unregister(handler);
        super.unregister(handler);
    }
}
