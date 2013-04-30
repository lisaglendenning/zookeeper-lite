package edu.uw.zookeeper.server;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.ServerExecutor;
import edu.uw.zookeeper.protocol.Message.ClientMessage;
import edu.uw.zookeeper.protocol.Message.ServerMessage;
import edu.uw.zookeeper.protocol.server.ServerProtocolConnection;

public class ConnectionHandler implements ServerExecutor {

    protected final ServerProtocolConnection connection;
    
    protected ConnectionHandler(ServerProtocolConnection connection) {
        this.connection = connection;
    }

    @Override
    public ListenableFuture<ServerMessage> submit(ClientMessage request) {
        // TODO Auto-generated method stub
        return null;
    }
}
