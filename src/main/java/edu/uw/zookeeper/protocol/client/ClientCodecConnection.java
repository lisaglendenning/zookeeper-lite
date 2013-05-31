package edu.uw.zookeeper.protocol.client;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class ClientCodecConnection extends ProtocolCodecConnection<Message.ClientSessionMessage, Message.ServerSessionMessage, ClientProtocolCodec> {

    public static ParameterizedFactory<Connection<Message.ClientSessionMessage>, ClientCodecConnection> factory() {
        return new ParameterizedFactory<Connection<Message.ClientSessionMessage>, ClientCodecConnection>() {
                    @Override
                    public ClientCodecConnection get(Connection<Message.ClientSessionMessage> value) {
                        return ClientCodecConnection.newInstance(value);
                    }
                };
    }

    public static ClientCodecConnection newInstance(
            Connection<Message.ClientSessionMessage> connection) {
        ClientProtocolCodec codec = ClientProtocolCodec.newInstance(connection);
        return newInstance(codec, connection);
    }
    
    protected static ClientCodecConnection newInstance(
            ClientProtocolCodec codec,
            Connection<Message.ClientSessionMessage> connection) {
        return new ClientCodecConnection(codec, connection);
    }
    
    protected ClientCodecConnection(
            ClientProtocolCodec codec, 
            Connection<Message.ClientSessionMessage> connection) {
        super(codec, connection);
    }
}