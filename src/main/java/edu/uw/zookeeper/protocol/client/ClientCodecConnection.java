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
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class ClientCodecConnection extends CodecConnection<Message.ClientSessionMessage, Message.ServerSessionMessage, ClientProtocolCodec> {

    public static Factory<? extends ClientCodecConnection> factory(
            Factory<Connection> connectionFactory,
            Factory<Publisher> publisherFactory) {
        return factory(connectionFactory, Builder.newInstance(publisherFactory));
    }

    public static Factory<? extends ClientCodecConnection> factory(
            Factory<Connection> connectionFactory,
            ParameterizedFactory<Connection, ? extends ClientCodecConnection> clientFactory) {
        return FromConnectionBuilder.newInstance(connectionFactory, clientFactory);
    }
    
    public static class FromConnectionBuilder implements Factory<ClientCodecConnection> {

        public static FromConnectionBuilder newInstance(
                Factory<Connection> connectionFactory,
                ParameterizedFactory<Connection, ? extends ClientCodecConnection> codecFactory) {
            return new FromConnectionBuilder(connectionFactory, codecFactory);
        }
        
        protected final Factory<Connection> connectionFactory;
        protected final ParameterizedFactory<Connection, ? extends ClientCodecConnection> codecFactory;
        
        protected FromConnectionBuilder(
                Factory<Connection> connectionFactory,
                ParameterizedFactory<Connection, ? extends ClientCodecConnection> codecFactory) {
            this.connectionFactory = connectionFactory;
            this.codecFactory = codecFactory;
        }
        
        @Override
        public ClientCodecConnection get() {
            Connection connection = connectionFactory.get();
            ClientCodecConnection client = codecFactory.get(connection);
            return client;
        }
        
    }
    
    public static class Builder implements ParameterizedFactory<Connection, ClientCodecConnection> {
    
        public static Builder newInstance(
                Factory<Publisher> publisherFactory) {
            return new Builder(publisherFactory);
        }
        
        private final Factory<Publisher> publisherFactory;
    
        private Builder(
                Factory<Publisher> publisherFactory) {
            super();
            this.publisherFactory = publisherFactory;
        }
    
        @Override
        public ClientCodecConnection get(Connection connection) {
            Publisher publisher = publisherFactory.get();
            return PingingClientCodecConnection.newInstance(
                    publisher, connection);
        }
    }

    public static ClientCodecConnection newInstance(Publisher publisher,
            Connection connection) {
        Automaton<ProtocolState, Message> automaton = EventfulAutomaton.createSynchronized(publisher, ProtocolState.ANONYMOUS);
        ClientProtocolCodec codec = ClientProtocolCodec.create(automaton);
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
}