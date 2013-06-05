package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.proto.IRequestHeader;


public class SessionRequestWrapper implements Operation.SessionRequest {

    public static Operation.SessionRequest newInstance(
            int xid,
            Operation.Request request) {
        return new SessionRequestWrapper(xid, request);
    }
    
    public static Operation.SessionRequest decode(InputStream input) throws IOException {
        IRequestHeader header = Records.Requests.Headers.decode(input);
        OpCode opcode = header.opcode();
        Operation.Request request = OpRecord.OpRequest.decode(opcode, input);
        return newInstance(header.getXid(), request);
    }
    
    private final int xid;
    private final Operation.Request request;

    private SessionRequestWrapper(int xid, Operation.Request request) {
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
    public ByteBuf encode(ByteBufAllocator output) throws IOException {
        Operation.Request request = request();
        ByteBuf out = output.buffer();
        Records.Requests.Headers.encode(xid(), request.opcode(),
                new ByteBufOutputStream(out));
        if (request instanceof Encodable) {
            ByteBuf payload = ((Encodable)request).encode(output);
            out = Buffers.composite(output, out, payload);
        }
        return out;
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
        SessionRequestWrapper other = (SessionRequestWrapper) obj;
        return Objects.equal(xid(), other.xid())
                && Objects.equal(request(), other.request());
    }
}
