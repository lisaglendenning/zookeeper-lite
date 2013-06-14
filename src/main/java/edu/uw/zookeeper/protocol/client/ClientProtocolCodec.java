package edu.uw.zookeeper.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import edu.uw.zookeeper.protocol.Decoder;
import edu.uw.zookeeper.protocol.Encodable;
import edu.uw.zookeeper.protocol.EncodableEncoder;
import edu.uw.zookeeper.protocol.Encoder;
import edu.uw.zookeeper.protocol.Frame;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.SessionReplyDecoder;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.Stateful;

/**
 * Implemented for the case where encode is called by a different thread than decode,
 * but it is not safe for multiple threads to call encode or multiple
 * threads to call decode.
 */
public class ClientProtocolCodec
    extends ProtocolCodec<Message.ClientSessionMessage, Message.ServerSessionMessage> {

    public static ClientProtocolCodec newInstance(
            Publisher publisher) {
        return newInstance(newAutomaton(publisher));
    }
    
    /**
     * 
     * @param automaton must be thread-safe
     */
    public static ClientProtocolCodec newInstance(
            Automaton<ProtocolState, Message> automaton) {
        Pending pending = Pending.newInstance();
        ClientProtocolEncoder encoder = ClientProtocolEncoder.create(automaton);
        ClientProtocolDecoder decoder = ClientProtocolDecoder.create(automaton, pending);
        return new ClientProtocolCodec(automaton, encoder, decoder, pending);
    }
    
    public static class Pending implements Function<Integer, OpCode>, Reference<Queue<Pair<Integer, OpCode>>> {
        public static Pending newInstance() {
            return new Pending(new LinkedBlockingQueue<Pair<Integer, OpCode>>());
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
    
    private final Pending pending;
    
    private ClientProtocolCodec(
            Automaton<ProtocolState, Message> automaton,
            Encoder<Message.ClientSessionMessage> encoder,
            Decoder<Optional<? extends Message.ServerSessionMessage>> decoder,
            Pending pending) {
        super(automaton, encoder, decoder);
        this.pending = pending;
    }

    /**
     * Don't call concurrently!
     */
    @Override
    public ByteBuf encode(Message.ClientSessionMessage input, ByteBufAllocator output) throws IOException {
        ByteBuf out = super.encode(input, output);
        // we only need to remember xid -> opcode of pending messages
        Pair<Integer, OpCode> pair = null;
        if (input instanceof Operation.XidHeader) {
            int xid = ((Operation.XidHeader)input).xid();
            if (! Records.OpCodeXid.has(xid)) {
                assert (input instanceof Operation.SessionRequest);
                OpCode opcode = ((Operation.SessionRequest)input).request().opcode();
                pair = Pair.create(xid, opcode);
                pending.get().add(pair);
            }
        }
        return out;
    }

    /**
     * Don't call concurrently!
     */
    @Override
    public Optional<? extends Message.ServerSessionMessage> decode(ByteBuf input)
            throws IOException {
        // the peek and poll need to be atomic
        Optional<? extends Message.ServerSessionMessage> out = super.decode(input);
        if (out.isPresent()) {
            Message.ServerSessionMessage reply = out.get();
            Pair<Integer, OpCode> next = pending.get().peek();
            if (next != null && reply instanceof Operation.XidHeader) {
                if (next.first().equals(((Operation.XidHeader)reply).xid())) {
                    pending.get().poll();
                }
            }
        }
        return out;
    }

    public static class ClientProtocolEncoder implements 
            Stateful<ProtocolState>,
            Encoder<Message.ClientSessionMessage> {
    
        public static ClientProtocolEncoder create(
                Stateful<ProtocolState> stateful) {
            return new ClientProtocolEncoder(stateful);
        }
        
        private final Stateful<ProtocolState> stateful;
        private final Encoder<Encodable> frameEncoder;
        
        private ClientProtocolEncoder(
                Stateful<ProtocolState> stateful) {
            this.stateful = stateful;
            this.frameEncoder = Frame.FramedEncoder.create(EncodableEncoder.getInstance());
        }
    
        @Override
        public ProtocolState state() {
            return stateful.state();
        }
        
        @Override
        public ByteBuf encode(Message.ClientSessionMessage input, ByteBufAllocator output) 
                throws IOException {
            ByteBuf out;
            ProtocolState state = state();
            switch (state) {
            case ANONYMOUS:
            case CONNECTING:
            case CONNECTED:
            case DISCONNECTING:
                out = frameEncoder.encode(input, output);
                break;
            default:
                throw new IllegalStateException(state.toString());
            }
            return out;
        }
    }
    
    public static class ClientProtocolDecoder implements 
            Stateful<ProtocolState>,
            Decoder<Optional<? extends Message.ServerSessionMessage>> {

        public static ClientProtocolDecoder create(
                Stateful<ProtocolState> stateful,
                Function<Integer, OpCode> xidToOpCode) {
            return new ClientProtocolDecoder(stateful, xidToOpCode);
        }

        private final Stateful<ProtocolState> stateful;
        private final Decoder<Optional<Message.ServerSessionMessage>> sessionDecoder;
        
        private ClientProtocolDecoder(
                Stateful<ProtocolState> stateful,
                Function<Integer, OpCode> xidToOpCode) {
            this.stateful = stateful;
            this.sessionDecoder = Frame.FramedDecoder.create(
                    Frame.FrameDecoder.getDefault(),
                    SessionReplyDecoder.create(stateful, xidToOpCode));
        }

        @Override
        public ProtocolState state() {
            return stateful.state();
        }
        
        @Override
        public Optional<? extends Message.ServerSessionMessage> decode(ByteBuf input) throws IOException {
            Optional<? extends Message.ServerSessionMessage> out;
            ProtocolState state = state();
            switch (state) {
            case CONNECTING:
            case CONNECTED:
            case DISCONNECTING:
                out = sessionDecoder.decode(input);
                break;
            default:
                throw new IllegalStateException(state.toString());
            }
            return out;
        }
    }
}
