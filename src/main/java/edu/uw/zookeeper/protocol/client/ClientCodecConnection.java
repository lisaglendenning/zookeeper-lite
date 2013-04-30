package edu.uw.zookeeper.protocol.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.CodecConnection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.EventfulAutomaton;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class ClientCodecConnection extends CodecConnection<Message.ClientSessionMessage, Message.ServerSessionMessage, ClientProtocolCodec> {

    public static ParameterizedFactory<Connection, ClientCodecConnection> factory(
            final Factory<Publisher> publisherFactory) {
        return new ParameterizedFactory<Connection, ClientCodecConnection>() {
                    @Override
                    public ClientCodecConnection get(Connection value) {
                        return ClientCodecConnection.newInstance(publisherFactory.get(), value);
                    }
                };
    }

    public static <T extends ClientCodecConnection> Factory<T> factory(
            final Factory<Connection> connectionFactory,
            final ParameterizedFactory<Connection, ? extends T> clientFactory) {
        return Factories.link(connectionFactory, clientFactory);
    }

    public static ClientCodecConnection newInstance(Publisher publisher,
            Connection connection) {
        Automaton<ProtocolState, Message> automaton = EventfulAutomaton.createSynchronized(publisher, ProtocolState.ANONYMOUS);
        ClientProtocolCodec codec = ClientProtocolCodec.newInstance(automaton);
        return newInstance(publisher, codec, connection);
    }
    
    public static ClientCodecConnection newInstance(Publisher publisher,
            ClientProtocolCodec codec,
            Connection connection) {
        return new ClientCodecConnection(publisher, codec, connection);
    }
    
    protected ClientCodecConnection(Publisher publisher,
            ClientProtocolCodec codec, Connection connection) {
        super(publisher, codec, connection);
    }
    
    @Override
    public void write(Message.ClientSessionMessage message) throws IOException {
        ProtocolState protocolState = asCodec().state();
        switch (protocolState) {
        case ANONYMOUS:
            checkArgument(message instanceof OpCreateSession.Request);
            break;
        case CONNECTING:
        case CONNECTED:
            break;
        default:
            throw new IllegalStateException(protocolState.toString());
        }
        super.write(message);
    }
    
    @Override
    protected void post(Object event) {
        super.post(event);
        
        ProtocolState state = asCodec().state();
        switch (state) {
        case DISCONNECTED:
        case ERROR:
            asConnection().close();
            break;
        default:
            break;
        }
    }
}