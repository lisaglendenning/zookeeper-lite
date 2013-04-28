package edu.uw.zookeeper.protocol.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;

import com.google.common.base.Optional;

import edu.uw.zookeeper.protocol.Codec;
import edu.uw.zookeeper.protocol.Decoder;
import edu.uw.zookeeper.protocol.Encodable;
import edu.uw.zookeeper.protocol.Encoder;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.Frame;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionRequestDecoder;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Stateful;

public class ServerProtocolCodec implements 
        Stateful<ProtocolState>,
        Codec<Message.ServerMessage, Optional<? extends Message.ClientMessage>> {

    public static ServerProtocolCodec create(Automaton<ProtocolState, Message> automaton) {
        return new ServerProtocolCodec(automaton);
    }
    
    private final Automaton<ProtocolState, Message> automaton;
    private final ServerProtocolEncoder encoder;
    private final ServerProtocolDecoder decoder;
    
    private ServerProtocolCodec(
            Automaton<ProtocolState, Message> automaton) {
        this.automaton = automaton;
        this.encoder = ServerProtocolEncoder.create(automaton);
        this.decoder = ServerProtocolDecoder.create(automaton);
    }
    
    @Override
    public ProtocolState state() {
        return automaton.state();
    }

    @Override
    public Optional<? extends Message.ClientMessage> decode(ByteBuf input)
            throws IOException {
        Optional<? extends Message.ClientMessage> out = decoder.decode(input);
        if (out.isPresent()) {
            automaton.apply(out.get());
        }
        return out;
    }

    @Override
    public ByteBuf encode(Message.ServerMessage input, ByteBufAllocator output) throws IOException {
        ByteBuf out = encoder.encode(input, output);
        automaton.apply(input);
        return out;
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
            this.frameEncoder = Frame.FrameEncoder.Singleton.getInstance();
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
            this.sessionDecoder = Frame.FrameDecoder.create( 
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
