package edu.uw.zookeeper.protocol.client;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.Stateful;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Decoder;
import edu.uw.zookeeper.protocol.Encoder;
import edu.uw.zookeeper.protocol.Frame;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;

/**
 * Implemented for the case where encode is called by a different thread than decode,
 * but it is not safe for multiple threads to call encode or multiple
 * threads to call decode.
 */
public class ClientProtocolCodec
    implements ProtocolCodec<Message.ClientSession, Message.ServerSession> {

    public static ClientProtocolCodec newInstance(
            Publisher publisher) {
        return newInstance(publisher, ProtocolState.ANONYMOUS);
    }
    
    public static ClientProtocolCodec newInstance(
            Publisher publisher, ProtocolState state) {
        Automatons.SynchronizedEventfulAutomaton<ProtocolState, Message> automaton =
                Automatons.createSynchronizedEventful(publisher, 
                        Automatons.createSimple(state));
        Pending pending = Pending.newInstance();
        Encoder<? super Message.ClientSession> encoder = 
                Frame.FramedEncoder.create(
                        ClientProtocolEncoder.newInstance(automaton));
        Decoder<Optional<Message.ServerSession>> decoder =
                Frame.FramedDecoder.create(
                        Frame.FrameDecoder.getDefault(),
                        ClientProtocolDecoder.newInstance(automaton, pending));
        return new ClientProtocolCodec(automaton, encoder, decoder, pending.get());
    }
    
    protected static class Pending implements Function<Integer, OpCode>, Reference<Queue<Pair<Integer, OpCode>>> {
        public static Pending newInstance() {
            return new Pending(new ConcurrentLinkedQueue<Pair<Integer, OpCode>>());
        }
        
        // must be thread-safe
        private final Queue<Pair<Integer, OpCode>> queue;
        
        public Pending(Queue<Pair<Integer, OpCode>> queue) {
            this.queue = queue;
        }

        @Override
        public OpCode apply(Integer xid) {
            Pair<Integer, OpCode> next = queue.peek();
            if (next == null) {
                throw new IllegalStateException(String.format("Unexpected xid (%d), no pending requests", xid));
            }
            if (xid.equals(next.first())) {
                return next.second();
            } else {
                throw new IllegalArgumentException(String.format("Unexpected xid (%d), expecting %s", xid, next));
            }
        }

        @Override
        public Queue<Pair<Integer, OpCode>> get() {
            return queue;
        }
    }

    protected final Automatons.SynchronizedEventfulAutomaton<ProtocolState, Message> automaton;
    protected final Encoder<? super Message.ClientSession> encoder;
    protected final Decoder<Optional<Message.ServerSession>> decoder;
    protected final Queue<Pair<Integer, OpCode>> pending;
    
    protected ClientProtocolCodec(
            Automatons.SynchronizedEventfulAutomaton<ProtocolState, Message> automaton,
            Encoder<? super Message.ClientSession> encoder,
            Decoder<Optional<Message.ServerSession>> decoder,
            Queue<Pair<Integer, OpCode>> pending) {
        this.automaton = automaton;
        this.encoder = encoder;
        this.decoder = decoder;
        this.pending = pending;
    }

    /**
     * Don't call concurrently!
     */
    @Override
    public void encode(Message.ClientSession input, ByteBuf output) throws IOException {
        encoder.encode(input, output);
        automaton.apply(input);
        // we only need to remember xid -> opcode of pending messages
        if (input instanceof Operation.RequestId) {
            int xid = ((Operation.RequestId)input).xid();
            if (! OpCodeXid.has(xid)) {
                assert (input instanceof Operation.ProtocolRequest);
                OpCode opcode = ((Operation.ProtocolRequest<?>) input).record().opcode();
                Pair<Integer, OpCode> pair = Pair.create(xid, opcode);
                pending.add(pair);
            }
        }
    }

    /**
     * Don't call concurrently!
     */
    @Override
    public Optional<Message.ServerSession> decode(ByteBuf input)
            throws IOException {
        Optional<Message.ServerSession> out =  decoder.decode(input);
        if (out.isPresent()) {
            automaton.apply(out.get());
            Message.ServerSession reply = out.get();
            // the peek and poll need to be atomic
            Pair<Integer, OpCode> next = pending.peek();
            if ((next != null) && (reply instanceof Operation.RequestId)) {
                if (next.first().equals(((Operation.RequestId)reply).xid())) {
                    pending.poll();
                }
            }
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

    public static class ClientProtocolEncoder implements 
            Stateful<ProtocolState>,
            Encoder<Message.ClientSession> {

        public static ClientProtocolEncoder newInstance(
                Stateful<ProtocolState> stateful) {
            return new ClientProtocolEncoder(stateful);
        }

        private final Stateful<ProtocolState> stateful;
        
        private ClientProtocolEncoder(
                Stateful<ProtocolState> stateful) {
            this.stateful = stateful;
        }

        @Override
        public ProtocolState state() {
            return stateful.state();
        }
        
        @Override
        public void encode(Message.ClientSession input, ByteBuf output) throws IOException {
            ProtocolState state = state();
            switch (state) {
            case ANONYMOUS:
            case CONNECTING:
            case CONNECTED:
            case DISCONNECTING:
                input.encode(output);
                break;
            default:
                throw new IllegalStateException(state.toString());
            }
        }
    }
    
    public static class ClientProtocolDecoder implements 
            Stateful<ProtocolState>,
            Decoder<Message.ServerSession> {

        public static ClientProtocolDecoder newInstance(
                Stateful<ProtocolState> stateful,
                Function<Integer, OpCode> xidToOpCode) {
            return new ClientProtocolDecoder(stateful, xidToOpCode);
        }

        private final Stateful<ProtocolState> stateful;
        private final Function<Integer, OpCode> xidToOpCode;
        
        private ClientProtocolDecoder(
                Stateful<ProtocolState> stateful,
                Function<Integer, OpCode> xidToOpCode) {
            this.stateful = stateful;
            this.xidToOpCode = xidToOpCode;
        }

        @Override
        public ProtocolState state() {
            return stateful.state();
        }
        
        @Override
        public Message.ServerSession decode(ByteBuf input) throws IOException {
            ProtocolState state = state();
            Message.ServerSession out;
            switch (state) {
            case CONNECTING:
                out = ConnectMessage.Response.decode(input);
                break;
            case CONNECTED:
            case DISCONNECTING:
                out = ProtocolResponseMessage.decode(xidToOpCode, input);
                break;
            default:
                throw new IllegalStateException(state.toString());
            }
            return out;
        }
    }
}
