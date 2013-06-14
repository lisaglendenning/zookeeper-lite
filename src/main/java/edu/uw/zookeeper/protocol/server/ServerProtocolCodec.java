package edu.uw.zookeeper.protocol.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;

import org.apache.jute.BinaryInputArchive;

import com.google.common.base.Optional;
import com.google.common.collect.Range;

import edu.uw.zookeeper.protocol.Decoder;
import edu.uw.zookeeper.protocol.Encodable;
import edu.uw.zookeeper.protocol.Encoder;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.Frame;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionRequestDecoder;
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
        ServerProtocolEncoder encoder = ServerProtocolEncoder.create(automaton);
        ServerProtocolDecoder decoder = ServerProtocolDecoder.create(automaton);
        return new ServerProtocolCodec(automaton, encoder, decoder);
    }
    
    private ServerProtocolCodec(
            Automaton<ProtocolState, Message> automaton,
            Encoder<Message.ServerMessage> encoder,
            Decoder<Optional<? extends Message.ClientMessage>> decoder) {
        super(automaton, encoder, decoder);
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
            this.frameEncoder = Frame.FramedEncoder.create();
        }

        @Override
        public ProtocolState state() {
            return stateful.state();
        }
        
        @Override
        public ByteBuf encode(Message.ServerMessage input, ByteBufAllocator output) throws IOException {
            ByteBuf out;
            ProtocolState state = state();
            if (input instanceof Message.ServerSessionMessage) {
                switch (state) {
                case CONNECTING:
                case CONNECTED:
                case DISCONNECTING:
                    out = frameEncoder.encode(input, output);
                    break;
                default:
                    throw new IllegalStateException(state.toString());
                }          
            } else {
                switch (state) {
                case ANONYMOUS:
                case CONNECTING:
                    out = input.encode(output);
                    break;
                default:
                    throw new IllegalStateException(state.toString());
                }
            }
            return out;
        }
    }

    public static class ServerProtocolDecoder implements 
            Stateful<ProtocolState>,
            Decoder<Optional<? extends Message.ClientMessage>> {
    
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
        public Optional<? extends Message.ClientMessage> decode(ByteBuf input) throws IOException {
            Optional<? extends Message.ClientMessage> out;
            ProtocolState state = state();
            switch (state) {
            case ANONYMOUS:
                out = FourLetterRequest.decode(input);
                if (! out.isPresent()) {
                    out = sessionDecoder.decode(input);
                }
                break;
            case CONNECTING:
            case CONNECTED:
                out = sessionDecoder.decode(input);
                break;
            default:
                throw new IllegalStateException(state.toString());
            }
            return out;
        }
    }
}
