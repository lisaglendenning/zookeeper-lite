package edu.uw.zookeeper.protocol.client;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Message.ClientSessionMessage;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class ClientCodecConnection extends ProtocolCodecConnection<Message.ClientSessionMessage, Message.ServerSessionMessage, ClientProtocolCodec> {

    public static ParameterizedFactory<Publisher, Pair<Class<Message.ClientSessionMessage>, ClientProtocolCodec>> codecFactory() {
        return new ParameterizedFactory<Publisher, Pair<Class<Message.ClientSessionMessage>, ClientProtocolCodec>>() {
            @Override
            public Pair<Class<ClientSessionMessage>, ClientProtocolCodec> get(
                    Publisher value) {
                return Pair.create(ClientSessionMessage.class, ClientProtocolCodec.newInstance(value));
            }
        };
    }

    public static ParameterizedFactory<Pair<Pair<Class<Message.ClientSessionMessage>, ClientProtocolCodec>, Connection<Message.ClientSessionMessage>>, ClientCodecConnection> factory() {
        return new ParameterizedFactory<Pair<Pair<Class<Message.ClientSessionMessage>, ClientProtocolCodec>, Connection<Message.ClientSessionMessage>>, ClientCodecConnection>() {
            @Override
            public ClientCodecConnection get(
                    Pair<Pair<Class<Message.ClientSessionMessage>, ClientProtocolCodec>, Connection<Message.ClientSessionMessage>> value) {
                return new ClientCodecConnection(value.first().second(), value.second());
            }
        };
    }
    
    protected ClientCodecConnection(
            ClientProtocolCodec codec, 
            Connection<Message.ClientSessionMessage> connection) {
        super(codec, connection);
    }
}