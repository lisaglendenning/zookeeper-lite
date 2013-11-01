package edu.uw.zookeeper.protocol.client;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.SessionListener;

public interface ConnectionClientExecutor<
        I extends Operation.Request,
        V extends Operation.ProtocolResponse<?>,
        T extends SessionListener,
        C extends ProtocolConnection<? super Message.ClientSession,?,?,?,?>> 
            extends ClientExecutor<I,V,T> {

    ListenableFuture<ConnectMessage.Response> session();
    
    C connection();
}
