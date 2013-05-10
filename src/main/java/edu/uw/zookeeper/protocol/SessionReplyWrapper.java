package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkArgument;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.io.InputStream;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Objects;

import edu.uw.zookeeper.data.Buffers;
import edu.uw.zookeeper.data.Encodable;
import edu.uw.zookeeper.protocol.proto.IReplyHeader;
import edu.uw.zookeeper.protocol.proto.Records;


public class SessionReplyWrapper implements Operation.SessionReply {

    public static Operation.SessionReply create(
            int xid,
            long zxid,
            Operation.Reply reply) {
        return new SessionReplyWrapper(zxid, xid, reply);
    }

    public static Operation.SessionReply decode(
            Function<Integer, OpCode> xidToOpCode, InputStream stream)
            throws IOException {
        IReplyHeader header = Records.Responses.Headers.decode(stream);
        KeeperException.Code err = KeeperException.Code
                .get(header.getErr());
        checkArgument(err != null);
        Operation.Reply reply;
        if (err == KeeperException.Code.OK) {
            int xid = header.getXid();
            OpCode opcode;
            if (Records.OpCodeXid.has(xid)) {
                opcode = Records.OpCodeXid.of(xid).opcode();
            } else {
                opcode = xidToOpCode.apply(xid);
            }
            
            switch (opcode) {
            case CREATE_SESSION:
                throw new IllegalArgumentException();
            default:
                reply = OpRecord.OpResponse.decode(opcode, stream);
                break;
            }
        } else {
            // FIXME: make sure there isn't anything following an
            // error header? doesn't look like the client expects one
            reply = OpError.create(err);
        }
        return SessionReplyWrapper.create(header.getXid(), header.getZxid(), reply);
    }
    
    private final long zxid;
    private final int xid;
    private final Operation.Reply reply;

    private SessionReplyWrapper(long zxid, int xid, Operation.Reply reply) {
        this.zxid = zxid;
        this.xid = xid;
        this.reply = reply;
    }
    
    @Override
    public long zxid() {
        return zxid;
    }

    @Override
    public int xid() {
        return xid;
    }

    @Override
    public Operation.Reply reply() {
        return reply;
    }

    @Override
    public ByteBuf encode(ByteBufAllocator output) throws IOException {
        Operation.Reply reply = reply();
        KeeperException.Code error = KeeperException.Code.OK;
        if (reply instanceof Operation.Error) {
            error = ((Operation.Error)reply).error();
        }
        ByteBuf out = output.buffer();
        Records.Responses.Headers.encode(xid(), zxid(), error,
                new ByteBufOutputStream(out));
        if (reply instanceof Encodable) {
            ByteBuf payload = ((Encodable)reply).encode(output);
            out = Buffers.composite(output, out, payload);
        }
        return out;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("xid", xid())
                .add("zxid", zxid())
                .add("reply", reply()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(xid(), zxid(), reply());
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
        SessionReplyWrapper other = (SessionReplyWrapper) obj;
        return Objects.equal(xid(), other.xid())
                && Objects.equal(zxid(), other.zxid())
                && Objects.equal(reply(), other.reply());
    }
}
