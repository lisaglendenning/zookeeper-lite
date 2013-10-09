package edu.uw.zookeeper.protocol.client;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;

public interface ConnectionClientExecutor<
        I extends Operation.Request,
        V extends Operation.ProtocolResponse<?>,
        C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> 
            extends ClientExecutor<I,V> {

    ListenableFuture<ConnectMessage.Response> session();
    
    C connection();
}
