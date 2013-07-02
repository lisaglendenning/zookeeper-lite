package edu.uw.zookeeper.protocol.client;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Function;
import com.google.common.base.Optional;

import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Decoder;
import edu.uw.zookeeper.protocol.Encoder;
import edu.uw.zookeeper.protocol.Frame;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionReplyMessage;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
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
        return newInstance(publisher, ProtocolState.ANONYMOUS);
    }
    
    public static ClientProtocolCodec newInstance(
            Publisher publisher, ProtocolState state) {
        return new ClientProtocolCodec(newAutomaton(publisher, state), Pending.newInstance());
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
            Pending pending) {
        super(automaton,
                Frame.FramedEncoder.create(ClientProtocolEncoder.newInstance(automaton)),
                Frame.FramedDecoder.create(
                        Frame.FrameDecoder.getDefault(),
                        ClientProtocolDecoder.newInstance(automaton, pending)));
        this.pending = pending;
    }

    /**
     * Don't call concurrently!
     */
    @Override
    public void encode(Message.ClientSessionMessage input, ByteBuf output) throws IOException {
        super.encode(input, output);
        // we only need to remember xid -> opcode of pending messages
        if (input instanceof Operation.XidHeader) {
            int xid = ((Operation.XidHeader)input).xid();
            if (! OpCodeXid.has(xid)) {
                assert (input instanceof Operation.SessionRequest);
                OpCode opcode = ((Operation.SessionRequest)input).request().opcode();
                Pair<Integer, OpCode> pair = Pair.create(xid, opcode);
                pending.get().add(pair);
            }
        }
    }

    /**
     * Don't call concurrently!
     */
    @Override
    public Optional<Message.ServerSessionMessage> decode(ByteBuf input)
            throws IOException {
        // the peek and poll need to be atomic
        Optional<Message.ServerSessionMessage> out = super.decode(input);
        if (out.isPresent()) {
            Message.ServerSessionMessage reply = out.get();
            Pair<Integer, OpCode> next = pending.get().peek();
            if ((next != null) && (reply instanceof Operation.XidHeader)) {
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
        public void encode(Message.ClientSessionMessage input, ByteBuf output) throws IOException {
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
            Decoder<Message.ServerSessionMessage> {

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
        public Message.ServerSessionMessage decode(ByteBuf input) throws IOException {
            ProtocolState state = state();
            Message.ServerSessionMessage out;
            switch (state) {
            case CONNECTING:
                out = ConnectMessage.Response.decode(input);
                break;
            case CONNECTED:
            case DISCONNECTING:
                out = SessionReplyMessage.decode(xidToOpCode, input);
                break;
            default:
                throw new IllegalStateException(state.toString());
            }
            return out;
        }
    }
}
