package edu.uw.zookeeper.protocol.server;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.apache.jute.BinaryInputArchive;

import com.google.common.base.Objects;
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
import edu.uw.zookeeper.util.Automatons;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Stateful;

public class ServerProtocolCodec implements ProtocolCodec<Message.Server, Message.Client> {
    
    public static ServerProtocolCodec newInstance(
            Publisher publisher) {
        return newInstance(publisher, ProtocolState.ANONYMOUS);
    }
    
    public static ServerProtocolCodec newInstance(
            Publisher publisher, ProtocolState state) {
        Automatons.SynchronizedEventfulAutomaton<ProtocolState, Message> automaton =
                Automatons.createSynchronizedEventful(publisher, 
                        Automatons.createSimple(state));
        return new ServerProtocolCodec(automaton, ServerProtocolEncoder.create(automaton), ServerProtocolDecoder.create(automaton));
    }

    protected final Automatons.SynchronizedEventfulAutomaton<ProtocolState, Message> automaton;
    protected final Encoder<? super Message.Server> encoder;
    protected final Decoder<Optional<Message.Client>> decoder;
    
    protected ServerProtocolCodec(
            Automatons.SynchronizedEventfulAutomaton<ProtocolState, Message> automaton,
            Encoder<? super Message.Server> encoder,
            Decoder<Optional<Message.Client>> decoder) {
        this.automaton = automaton;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    /**
     * Don't call concurrently!
     */
    @Override
    public void encode(Message.Server input, ByteBuf output) throws IOException {
        encoder.encode(input, output);
        automaton.apply(input);
    }

    /**
     * Don't call concurrently!
     */
    @Override
    public Optional<Message.Client> decode(ByteBuf input)
            throws IOException {
        Optional<Message.Client> out =  decoder.decode(input);
        if (out.isPresent()) {
            automaton.apply(out.get());
        }
        return out;
    }

    @Override
    public ProtocolState state() {
        return automaton.state();
    }

    @Override
    public void register(Object handler) {
        automaton.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        automaton.unregister(handler);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("state", state()).toString();
    }

    public static class ServerProtocolEncoder implements 
            Stateful<ProtocolState>,
            Encoder<Message.Server> {

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
        public void encode(Message.Server input, ByteBuf output) throws IOException {
            ProtocolState state = state();
            if (input instanceof Message.ServerSession) {
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
            Decoder<Optional<Message.Client>> {
    
        public static ServerProtocolDecoder create(
                Stateful<ProtocolState> stateful) {
            return new ServerProtocolDecoder(stateful);
        }
        
        private final Stateful<ProtocolState> stateful;
        private final Decoder<Optional<Message.ClientSession>> sessionDecoder;
        
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
        public Optional<Message.Client> decode(ByteBuf input) throws IOException {
            Message.Client out = null;
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
            Stateful<ProtocolState>, Decoder<Message.ClientSession> {

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
        public Message.ClientSession decode(ByteBuf input)
                throws IOException {
            ProtocolState state = state();
            Message.ClientSession output;
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
