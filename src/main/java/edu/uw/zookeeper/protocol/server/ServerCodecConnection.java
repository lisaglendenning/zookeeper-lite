package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class ServerCodecConnection extends ProtocolCodecConnection<Message.ServerMessage, Message.ClientMessage, ServerProtocolCodec> {

    public static ParameterizedFactory<Publisher, Pair<Class<Message.ServerMessage>, ServerProtocolCodec>> codecFactory() {
        return new ParameterizedFactory<Publisher, Pair<Class<Message.ServerMessage>, ServerProtocolCodec>>() {
            @Override
            public Pair<Class<Message.ServerMessage>, ServerProtocolCodec> get(
                    Publisher value) {
                return Pair.create(Message.ServerMessage.class, ServerProtocolCodec.newInstance(value));
            }
        };
    }

    public static ParameterizedFactory<Pair<Pair<Class<Message.ServerMessage>, ServerProtocolCodec>, Connection<Message.ServerMessage>>, ServerCodecConnection> factory() {
        return new ParameterizedFactory<Pair<Pair<Class<Message.ServerMessage>, ServerProtocolCodec>, Connection<Message.ServerMessage>>, ServerCodecConnection>() {
            @Override
            public ServerCodecConnection get(
                    Pair<Pair<Class<Message.ServerMessage>, ServerProtocolCodec>, Connection<Message.ServerMessage>> value) {
                return new ServerCodecConnection(value.first().second(), value.second());
            }
        };
    }
    
    protected ServerCodecConnection(
            ServerProtocolCodec codec, Connection<Message.ServerMessage> connection) {
        super(codec, connection);
    }
}