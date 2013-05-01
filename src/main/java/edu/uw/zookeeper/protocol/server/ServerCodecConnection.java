package edu.uw.zookeeper.protocol.server;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;

import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.CodecConnection;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.EventfulAutomaton;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class ServerCodecConnection extends CodecConnection<Message.ServerMessage, Message.ClientMessage, ServerProtocolCodec> {

    public static ParameterizedFactory<Connection, ServerCodecConnection> factory(
            final Factory<Publisher> publisherFactory) {
        return new ParameterizedFactory<Connection, ServerCodecConnection>() {
                    @Override
                    public ServerCodecConnection get(Connection value) {
                        return ServerCodecConnection.newInstance(publisherFactory.get(), value);
                    }
                };
    }

    public static ServerCodecConnection newInstance(Publisher publisher,
            Connection connection) {
        Automaton<ProtocolState, Message> automaton = EventfulAutomaton.createSynchronized(publisher, ProtocolState.ANONYMOUS);
        ServerProtocolCodec codec = ServerProtocolCodec.create(automaton);
        return newInstance(publisher, codec, connection);
    }
    
    public static ServerCodecConnection newInstance(Publisher publisher,
            ServerProtocolCodec codec,
            Connection connection) {
        return new ServerCodecConnection(publisher, codec, connection);
    }
    
    private ServerCodecConnection(Publisher publisher,
            ServerProtocolCodec codec, Connection connection) {
        super(publisher, codec, connection);
    }
    
    @Override
    public void write(Message.ServerMessage message) throws IOException {
        ProtocolState protocolState = asCodec().state();
        switch (protocolState) {
        case ANONYMOUS:
            checkArgument(message instanceof FourLetterResponse);
            break;
        case CONNECTING:
            checkArgument(message instanceof OpCreateSession.Response);
            break;
        case CONNECTED:
        case DISCONNECTING:
            checkArgument(message instanceof Operation.SessionReply);
            break;
        default:
            throw new IllegalStateException(protocolState.toString());
        }
        super.write(message);
        
        ProtocolState state = asCodec().state();
        switch (state) {
        case DISCONNECTED:
        case ERROR:
            {
                asConnection().flush().addListener(new Runnable() {
                    @Override
                    public void run() {
                        asConnection().close();
                    }
                }, MoreExecutors.sameThreadExecutor());
            }
            break;
        default:
            break;
        }
    }
}