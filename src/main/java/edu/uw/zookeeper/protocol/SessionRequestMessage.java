package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import java.io.IOException;

import edu.uw.zookeeper.protocol.proto.ByteBufInputArchive;
import edu.uw.zookeeper.protocol.proto.ByteBufOutputArchive;
import edu.uw.zookeeper.protocol.proto.IRequestHeader;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.AbstractPair;


public class SessionRequestMessage extends AbstractPair<IRequestHeader, Records.Request> implements Message.ClientRequest {

    public static SessionRequestMessage newInstance(
            Records.Request request) {
        if (request instanceof Operation.RequestId) {
            return newInstance(((Operation.RequestId) request).xid(), request);
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static SessionRequestMessage newInstance(
            int xid,
            Records.Request request) {
        IRequestHeader header = Records.Requests.Headers.newInstance(xid, request.opcode());
        return new SessionRequestMessage(header, request);
    }

    public static SessionRequestMessage decode(ByteBuf input) throws IOException {
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
        return new SessionRequestMessage(header, request);
    }
    
    protected SessionRequestMessage(IRequestHeader header, Records.Request request) {
        super(header, request);
    }

    @Override
    public int xid() {
        return first.getXid();
    }

    @Override
    public Records.Request request() {
        return second;
    }

    @Override
    public void encode(ByteBuf output) throws IOException {
        ByteBufOutputArchive archive = new ByteBufOutputArchive(output);
        Records.Requests.Headers.serialize(first, archive);
        Records.Requests.serialize(second, archive);
    }
}
