package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class ServerCodecConnection extends ProtocolCodecConnection<Message.ServerMessage, Message.ClientMessage, ServerProtocolCodec> {

    public static ParameterizedFactory<Connection<Message.ServerMessage>, ServerCodecConnection> factory(
            final Factory<Publisher> publisherFactory) {
        return new ParameterizedFactory<Connection<Message.ServerMessage>, ServerCodecConnection>() {
                    @Override
                    public ServerCodecConnection get(Connection<Message.ServerMessage> value) {
                        return ServerCodecConnection.newInstance(publisherFactory.get(), value);
                    }
                };
    }

    public static ServerCodecConnection newInstance(
            Publisher publisher,
            Connection<Message.ServerMessage> connection) {
        ServerProtocolCodec codec = ServerProtocolCodec.newInstance(publisher);
        return newInstance(publisher, codec, connection);
    }
    
    public static ServerCodecConnection newInstance(
            Publisher publisher,
            ServerProtocolCodec codec,
            Connection<Message.ServerMessage> connection) {
        return new ServerCodecConnection(codec, connection);
    }
    
    protected ServerCodecConnection(
            ServerProtocolCodec codec, Connection<Message.ServerMessage> connection) {
        super(codec, connection);
    }
}