package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import java.io.IOException;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.protocol.proto.ByteBufInputArchive;
import edu.uw.zookeeper.protocol.proto.ByteBufOutputArchive;
import edu.uw.zookeeper.protocol.proto.IRequestHeader;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;


public class ProtocolRequestMessage<T extends Records.Request> extends AbstractPair<IRequestHeader, T> implements Message.ClientRequest<T> {

    public static <T extends Records.Request> ProtocolRequestMessage<T> from(
            T request) {
        if (request instanceof Operation.RequestId) {
            return of(((Operation.RequestId) request).getXid(), request);
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static <T extends Records.Request> ProtocolRequestMessage<T> of(
            int xid,
            T request) {
        IRequestHeader header = Records.Requests.Headers.newInstance(xid, request.getOpcode());
        return of(header, request);
    }

    public static <T extends Records.Request> ProtocolRequestMessage<T> of(
            IRequestHeader header,
            T request) {
         return new ProtocolRequestMessage<T>(header, request);
    }

    public static ProtocolRequestMessage<?> decode(ByteBuf input) throws IOException {
        ByteBufInputArchive archive = new ByteBufInputArchive(input);
        IRequestHeader header = Records.Requests.Headers.deserialize(archive);
        OpCode opcode = OpCode.of(header.getType());
        Records.Request request;
        switch (opcode) {
        case CREATE_SESSION:
            throw new IllegalArgumentException(opcode.toString());
        case PING:
            request = Ping.Request.newInstance();
            break;
        default:
            request = Records.Requests.deserialize(opcode, archive);
            break;
        }
        return of(header, request);
    }
    
    protected ProtocolRequestMessage(IRequestHeader header, T request) {
        super(header, request);
    }

    @Override
    public int getXid() {
        return first.getXid();
    }

    @Override
    public T getRecord() {
        return second;
    }

    @Override
    public void encode(ByteBuf output) throws IOException {
        ByteBufOutputArchive archive = new ByteBufOutputArchive(output);
        Records.Requests.Headers.serialize(first, archive);
        Records.Requests.serialize(second, archive);
    }
}
