package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.proto.ByteBufInputArchive;
import edu.uw.zookeeper.protocol.proto.ByteBufOutputArchive;
import edu.uw.zookeeper.protocol.proto.IRequestHeader;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;


public class SessionRequestMessage implements Operation.SessionRequest {

    public static SessionRequestMessage newInstance(
            int xid,
            Operation.Request request) {
        return new SessionRequestMessage(xid, request);
    }

    public static SessionRequestMessage decode(ByteBuf input) throws IOException {
        ByteBufInputArchive archive = new ByteBufInputArchive(input);
        IRequestHeader header = Records.Requests.Headers.deserialize(archive);
        OpCode opcode = OpCode.of(header.getType());
        Operation.Request request;
        switch (opcode) {
        case PING:
            request = OpPing.Request.newInstance();
            break;
        default:
            request = Records.Requests.deserialize(opcode, archive);
            break;
        }
        return newInstance(header.getXid(), request);
    }
    
    private final int xid;
    private final Operation.Request request;

    public SessionRequestMessage(int xid, Operation.Request request) {
        this.xid = xid;
        this.request = request;
    }

    @Override
    public int xid() {
        return xid;
    }

    @Override
    public Operation.Request request() {
        return request;
    }

    @Override
    public void encode(ByteBuf output) throws IOException {
        ByteBufOutputArchive archive = new ByteBufOutputArchive(output);
        Records.Requests.Headers.serialize(xid(), request().opcode(),
                archive);
        Records.Requests.serialize(request(), archive);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("xid", xid())
                .add("request", request()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(xid(), request());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SessionRequestMessage other = (SessionRequestMessage) obj;
        return Objects.equal(xid(), other.xid())
                && Objects.equal(request(), other.request());
    }
}
