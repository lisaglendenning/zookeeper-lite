package edu.uw.zookeeper.protocol.server;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.apache.jute.BinaryInputArchive;

import com.google.common.base.Optional;
import com.google.common.collect.Range;

import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Decoder;
import edu.uw.zookeeper.protocol.Encodable;
import edu.uw.zookeeper.protocol.EncodableEncoder;
import edu.uw.zookeeper.protocol.Encoder;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.Frame;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionRequestMessage;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Stateful;

public class ServerProtocolCodec extends ProtocolCodec<Message.ServerMessage, Message.ClientMessage> {
    
    public static ServerProtocolCodec newInstance(
            Publisher publisher) {
        return newInstance(newAutomaton(publisher));
    }
    
    public static ServerProtocolCodec newInstance(
            Automaton<ProtocolState, Message> automaton) {
        return new ServerProtocolCodec(automaton);
    }
    
    protected ServerProtocolCodec(
            Automaton<ProtocolState, Message> automaton) {
        super(automaton, ServerProtocolEncoder.create(automaton), ServerProtocolDecoder.create(automaton));
    }
    
    public static class ServerProtocolEncoder implements 
            Stateful<ProtocolState>,
            Encoder<Message.ServerMessage> {

        public static ServerProtocolEncoder create(
                Stateful<ProtocolState> stateful) {
            return new ServerProtocolEncoder(stateful);
        }
        
        private final Stateful<ProtocolState> stateful;
        private final Encoder<Encodable> frameEncoder;
        
        private ServerProtocolEncoder(
                Stateful<ProtocolState> stateful) {
            this.stateful = stateful;
            this.frameEncoder = Frame.FramedEncoder.create(EncodableEncoder.getInstance());
        }

        @Override
        public ProtocolState state() {
            return stateful.state();
        }
        
        @Override
        public void encode(Message.ServerMessage input, ByteBuf output) throws IOException {
            ProtocolState state = state();
            if (input instanceof Message.ServerSessionMessage) {
                switch (state) {
                case CONNECTING:
                case CONNECTED:
                case DISCONNECTING:
                    frameEncoder.encode(input, output);
                    break;
                default:
                    throw new IllegalStateException(state.toString());
                }          
            } else {
                switch (state) {
                case ANONYMOUS:
                case CONNECTING:
                    input.encode(output);
                    break;
                default:
                    throw new IllegalStateException(state.toString());
                }
            }
        }
    }

    public static class ServerProtocolDecoder implements 
            Stateful<ProtocolState>,
            Decoder<Optional<Message.ClientMessage>> {
    
        public static ServerProtocolDecoder create(
                Stateful<ProtocolState> stateful) {
            return new ServerProtocolDecoder(stateful);
        }
        
        private final Stateful<ProtocolState> stateful;
        private final Decoder<Optional<Message.ClientSessionMessage>> sessionDecoder;
        
        private ServerProtocolDecoder(
                Stateful<ProtocolState> stateful) {
            this.stateful = stateful;
            this.sessionDecoder = Frame.FramedDecoder.create(
                    Frame.FrameDecoder.create(Range.closed(Integer.valueOf(0), Integer.valueOf(BinaryInputArchive.maxBuffer))),
                    SessionRequestDecoder.create(stateful));
        }

        @Override
        public ProtocolState state() {
            return stateful.state();
        }
        
        @Override
        public Optional<Message.ClientMessage> decode(ByteBuf input) throws IOException {
            Message.ClientMessage out = null;
            ProtocolState state = state();
            switch (state) {
            case ANONYMOUS:
                out = FourLetterRequest.decode(input).orNull();
                if (out == null) {
                    out = sessionDecoder.decode(input).orNull();
                }
                break;
            case CONNECTING:
            case CONNECTED:
                out = sessionDecoder.decode(input).orNull();
                break;
            default:
                throw new IllegalStateException(state.toString());
            }
            return Optional.fromNullable(out);
        }
    }

    public static class SessionRequestDecoder implements
            Stateful<ProtocolState>, Decoder<Message.ClientSessionMessage> {

        public static SessionRequestDecoder create(
                Stateful<ProtocolState> stateful) {
            return new SessionRequestDecoder(stateful);
        }

        private final Stateful<ProtocolState> stateful;

        private SessionRequestDecoder(Stateful<ProtocolState> stateful) {
            this.stateful = stateful;
        }

        @Override
        public ProtocolState state() {
            return stateful.state();
        }

        @Override
        public Message.ClientSessionMessage decode(ByteBuf input)
                throws IOException {
            ProtocolState state = state();
            Message.ClientSessionMessage output;
            switch (state) {
            case ANONYMOUS:
                output = ConnectMessage.Request.decode(input);
                break;
            case CONNECTING:
            case CONNECTED:
                output = SessionRequestMessage.decode(input);
                break;
            default:
                throw new IllegalStateException(state.toString());
            }
            return output;
        }
    }
}
