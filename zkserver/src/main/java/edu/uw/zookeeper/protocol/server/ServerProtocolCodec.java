package edu.uw.zookeeper.protocol.server;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.apache.jute.BinaryInputArchive;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Range;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Automatons.AutomatonListener;
import edu.uw.zookeeper.common.Stateful;
import edu.uw.zookeeper.net.Decoder;
import edu.uw.zookeeper.net.Encoder;
import edu.uw.zookeeper.protocol.ProtocolMessageAutomaton;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Encodable;
import edu.uw.zookeeper.protocol.EncodableEncoder;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.Frame;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.TelnetCloseRequest;

public class ServerProtocolCodec implements ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client> {
    
    public static ServerProtocolCodec defaults() {
        return newInstance(ProtocolState.ANONYMOUS);
    }
    
    public static ServerProtocolCodec newInstance(ProtocolState state) {
        Automatons.SynchronizedEventfulAutomaton<ProtocolState,Object,?> automaton =
                Automatons.createSynchronizedEventful(
                        Automatons.createEventful(
                        ProtocolMessageAutomaton.asAutomaton(state)));
        return new ServerProtocolCodec(automaton, ServerProtocolEncoder.create(automaton), ServerProtocolDecoder.create(automaton));
    }

    protected final Automatons.EventfulAutomaton<ProtocolState, Object> automaton;
    protected final Encoder<? super Message.Server, ?> encoder;
    protected final Decoder<Optional<Message.Client>, ?> decoder;
    
    protected ServerProtocolCodec(
            Automatons.EventfulAutomaton<ProtocolState, Object> automaton,
            Encoder<? super Message.Server, ?> encoder,
            Decoder<Optional<Message.Client>, ?> decoder) {
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
    public Class<? extends Message.Server> encodeType() {
        return Message.Server.class;
    }

    @Override
    public Class<? extends Message.Client> decodeType() {
        return Message.Client.class;
    }

    @Override
    public void subscribe(AutomatonListener<ProtocolState> listener) {
        automaton.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(AutomatonListener<ProtocolState> listener) {
        return automaton.unsubscribe(listener);
    }

    @Override
    public Optional<Automaton.Transition<ProtocolState>> apply(
            ProtocolState input) {
        return automaton.apply(input);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("state", state()).toString();
    }

    public static class ServerProtocolEncoder implements 
            Stateful<ProtocolState>,
            Encoder<Message.Server, Message.Server> {

        public static ServerProtocolEncoder create(
                Stateful<ProtocolState> stateful) {
            return new ServerProtocolEncoder(stateful);
        }
        
        private final Stateful<ProtocolState> stateful;
        private final Encoder<Encodable, ?> frameEncoder;
        
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
        public Class<? extends Message.Server> encodeType() {
            return Message.Server.class;
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
            Decoder<Optional<Message.Client>, Message.Client> {
    
        public static ServerProtocolDecoder create(
                Stateful<ProtocolState> stateful) {
            return new ServerProtocolDecoder(stateful);
        }
        
        private final Stateful<ProtocolState> stateful;
        private final Decoder<Optional<Message.ClientSession>, ?> sessionDecoder;
        
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
        public Class<? extends Message.Client> decodeType() {
            return Message.Client.class;
        }

        @Override
        public Optional<Message.Client> decode(ByteBuf input) throws IOException {
            Message.Client out = null;
            ProtocolState state = state();
            switch (state) {
            case ANONYMOUS:
                out = FourLetterRequest.decode(input).orNull();
                if (out == null) {
                    out = TelnetCloseRequest.decode(input).orNull();
                    if (out == null) {
                        out = sessionDecoder.decode(input).orNull();
                    }
                }
                break;
            case CONNECTING:
                // don't decode messages until we are connected
                break;
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
            Stateful<ProtocolState>, Decoder<Message.ClientSession, Message.ClientSession> {

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
        public Class<? extends Message.ClientSession> decodeType() {
            return Message.ClientSession.class;
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
                output = ProtocolRequestMessage.decode(input);
                break;
            default:
                throw new IllegalStateException(state.toString());
            }
            return output;
        }
    }
}
