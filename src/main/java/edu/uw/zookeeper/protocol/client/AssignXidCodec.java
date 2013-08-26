package edu.uw.zookeeper.protocol.client;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;

public class AssignXidCodec implements ProtocolCodec<Operation.Request, Message.ServerSession> {
    
    public static ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> factory() {
        return new ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>>() {
            @Override
            public Pair<Class<Operation.Request>, AssignXidCodec> get(
                    Publisher value) {
                return Pair.create(Operation.Request.class, AssignXidCodec.newInstance(
                        AssignXidProcessor.newInstance(),
                        ClientProtocolCodec.newInstance(value)));
            }
        };
    }
    
    public static AssignXidCodec newInstance(
            AssignXidProcessor xids,
            Publisher publisher) {
        return newInstance(xids, ClientProtocolCodec.newInstance(publisher));
    }
    
    public static AssignXidCodec newInstance(
            AssignXidProcessor xids,
            ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate) {
        return new AssignXidCodec(xids, delegate);
    }
    
    protected final AssignXidProcessor xids;
    protected final ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate;
    
    public AssignXidCodec(
            AssignXidProcessor xids, 
            ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate) {
        super();
        this.xids = xids;
        this.delegate = delegate;
    }
    
    public AssignXidProcessor xids() {
        return xids;
    }
    
    @Override
    public void encode(Operation.Request input, ByteBuf output) throws IOException {
        Message.ClientSession message = xids.apply(input);
        delegate.encode(message, output);
    }

    @Override
    public Optional<Message.ServerSession> decode(ByteBuf input)
            throws IOException {
        return delegate.decode(input);
    }

    @Override
    public ProtocolState state() {
        return delegate.state();
    }

    @Override
    public void register(Object handler) {
        delegate.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        delegate.unregister(handler);
    }
}
