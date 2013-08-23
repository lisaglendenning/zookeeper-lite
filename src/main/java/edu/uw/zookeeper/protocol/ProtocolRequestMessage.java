package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;

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
            return of(((Operation.RequestId) request).xid(), request);
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static <T extends Records.Request> ProtocolRequestMessage<T> of(
            int xid,
            T request) {
        IRequestHeader header = Records.Requests.Headers.newInstance(xid, request.opcode());
        return of(header, request);
    }

    public static <T extends Records.Request> ProtocolRequestMessage<T> of(
            IRequestHeader header,
            T request) {
         return new ProtocolRequestMessage<T>(header, request);
    }

    public static ProtocolRequestMessage<?> decode(ByteBuf input) throws IOException {
        return deserialize(new ByteBufInputArchive(input));
    }

    public static ProtocolRequestMessage<?> deserialize(InputArchive archive) throws IOException {
        IRequestHeader header = Records.Requests.Headers.deserialize(archive);
        OpCode opcode = OpCode.of(header.getType());
        Records.Request request;
        switch (opcode) {
        case CREATE_SESSION:
            throw new IllegalArgumentException(String.valueOf(opcode));
        default:
            request = Records.Requests.deserialize(opcode, archive);
            break;
        }
        return of(header, request);
    }
    
    public static void serialize(Operation.ProtocolRequest<?> value, OutputArchive archive) throws IOException {
        if (value instanceof ProtocolRequestMessage) {
            ((ProtocolRequestMessage<?>) value).serialize(archive);
        } else {
            IRequestHeader header = Records.Requests.Headers.newInstance(value.xid(), value.record().opcode());
            Records.Request record = value.record();
            Records.Requests.Headers.serialize(header, archive);
            Records.Requests.serialize(record, archive);
        }
    }
    
    protected ProtocolRequestMessage(IRequestHeader header, T request) {
        super(header, request);
    }
    
    public IRequestHeader header() {
        return first;
    }

    @Override
    public int xid() {
        return first.getXid();
    }

    @Override
    public T record() {
        return second;
    }

    @Override
    public void encode(ByteBuf output) throws IOException {
        ByteBufOutputArchive archive = new ByteBufOutputArchive(output);
        serialize(archive);
    }
    
    public void serialize(OutputArchive archive) throws IOException {
        Records.Requests.Headers.serialize(first, archive);
        Records.Requests.serialize(second, archive);
    }
}
