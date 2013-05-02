package edu.uw.zookeeper.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Function;
import com.google.common.base.Optional;

import edu.uw.zookeeper.protocol.Codec;
import edu.uw.zookeeper.protocol.Decoder;
import edu.uw.zookeeper.protocol.Encodable;
import edu.uw.zookeeper.protocol.Encoder;
import edu.uw.zookeeper.protocol.Frame;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.SessionReplyDecoder;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Stateful;

/**
 * Implemented for the case where encode is called by a different thread than decode,
 * but it is not safe for multiple threads to call encode or multiple
 * threads to call decode.
 */
public class ClientProtocolCodec implements 
        Stateful<ProtocolState>,
        Function<Integer, OpCode>,
        Codec<Message.ClientSessionMessage, Optional<? extends Message.ServerSessionMessage>> {

    /**
     * 
     * @param automaton must be thread-safe
     */
    public static ClientProtocolCodec newInstance(
            Automaton<ProtocolState, Message> automaton) {
        return new ClientProtocolCodec(automaton);
    }
    
    // must be thread-safe
    private final Automaton<ProtocolState, Message> automaton;
    private final ClientProtocolEncoder encoder;
    private final ClientProtocolDecoder decoder;
    // must be thread-safe
    private final Queue<Pair<Integer, OpCode>> pending;
    
    private ClientProtocolCodec(
            Automaton<ProtocolState, Message> automaton) {
        this.automaton = automaton;
        this.encoder = ClientProtocolEncoder.create(automaton);
        this.decoder = ClientProtocolDecoder.create(automaton, this);
        this.pending = new LinkedBlockingQueue<Pair<Integer, OpCode>>();
    }
    
    @Override
    public OpCode apply(Integer xid) {
        Pair<Integer, OpCode> next = pending.peek();
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
    public ProtocolState state() {
        return automaton.state();
    }
    
    /**
     * Don't call concurrently!
     */
    @Override
    public ByteBuf encode(Message.ClientSessionMessage input, ByteBufAllocator output) throws IOException {
        automaton.apply(input);
        ByteBuf out = encoder.encode(input, output);
        // we only need to remember xid -> opcode of pending messages
        Pair<Integer, OpCode> pair = null;
        if (input instanceof Operation.XidHeader) {
            int xid = ((Operation.XidHeader)input).xid();
            if (! Records.OpCodeXid.has(xid)) {
                assert (input instanceof Operation.SessionRequest);
                OpCode opcode = ((Operation.SessionRequest)input).request().opcode();
                pair = Pair.create(xid, opcode);
                pending.add(pair);
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
        Optional<? extends Message.ServerSessionMessage> out = decoder.decode(input);
        if (out.isPresent()) {
            Message.ServerSessionMessage reply = out.get();
            Pair<Integer, OpCode> next = pending.peek();
            if (next != null && reply instanceof Operation.XidHeader) {
                if (next.first().equals(((Operation.XidHeader)reply).xid())) {
                    pending.poll();
                }
            }
            automaton.apply(reply);
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
            this.frameEncoder = Frame.FrameEncoder.Singleton.getInstance();
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
            Decoder<Optional<? extends Message.ServerMessage>> {

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
            this.sessionDecoder = Frame.FrameDecoder.create( 
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
