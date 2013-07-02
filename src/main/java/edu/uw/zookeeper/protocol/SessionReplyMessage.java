package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.proto.ByteBufInputArchive;
import edu.uw.zookeeper.protocol.proto.ByteBufOutputArchive;
import edu.uw.zookeeper.protocol.proto.IErrorResponse;
import edu.uw.zookeeper.protocol.proto.IReplyHeader;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;


public class SessionReplyMessage implements Operation.SessionReply {

    public static SessionReplyMessage newInstance(
            int xid,
            long zxid,
            Operation.Response reply) {
        return new SessionReplyMessage(zxid, xid, reply);
    }

    public static SessionReplyMessage decode(
            Function<Integer, OpCode> xidToOpCode, ByteBuf input)
            throws IOException {
        ByteBufInputArchive archive = new ByteBufInputArchive(input);
        IReplyHeader header = Records.Responses.Headers.deserialize(archive);
        int err = header.getErr();
        Operation.Response reply;
        if (KeeperException.Code.OK.intValue() == err) {
            int xid = header.getXid();
            OpCode opcode;
            if (OpCodeXid.has(xid)) {
                opcode = OpCodeXid.of(xid).opcode();
            } else {
                opcode = xidToOpCode.apply(xid);
            }
            switch (opcode) {
            case PING:
                reply = OpPing.Response.newInstance();
                break;
            default:
                reply = Records.Responses.deserialize(opcode, archive);
                break;
            }
        } else {
            // FIXME: make sure there isn't anything following an
            // error header? doesn't look like the client expects one
            reply = new IErrorResponse(err);
        }
        return newInstance(header.getXid(), header.getZxid(), reply);
    }
    
    // TODO: DRY
    public static SessionReplyMessage decode(OpCode opcode, ByteBuf input)
            throws IOException {
        ByteBufInputArchive archive = new ByteBufInputArchive(input);
        IReplyHeader header = Records.Responses.Headers.deserialize(archive);
        int err = header.getErr();
        Operation.Response reply;
        if (KeeperException.Code.OK.intValue() == err) {
            int xid = header.getXid();
            if (OpCodeXid.has(xid)) {
                opcode = OpCodeXid.of(xid).opcode();
            }
            switch (opcode) {
            case PING:
                reply = OpPing.Response.newInstance();
                break;
            default:
                reply = Records.Responses.deserialize(opcode, archive);
                break;
            }
        } else {
            // FIXME: make sure there isn't anything following an
            // error header? doesn't look like the client expects one
            reply = new IErrorResponse(err);
        }
        return newInstance(header.getXid(), header.getZxid(), reply);
    }
    
    private final long zxid;
    private final int xid;
    private final Operation.Response reply;

    public SessionReplyMessage(long zxid, int xid, Operation.Response reply) {
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
    public Operation.Response reply() {
        return reply;
    }

    @Override
    public void encode(ByteBuf output) throws IOException {
        ByteBufOutputArchive archive = new ByteBufOutputArchive(output);
        KeeperException.Code error = KeeperException.Code.OK;
        if (reply() instanceof Operation.Error) {
            error = ((Operation.Error)reply()).error();
        }
        Records.Responses.Headers.serialize(xid(), zxid(), error, archive);
        if (KeeperException.Code.OK == error) {
            // don't serialize errors
            Records.Responses.serialize(reply(), archive);
        }
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
        SessionReplyMessage other = (SessionReplyMessage) obj;
        return Objects.equal(xid(), other.xid())
                && Objects.equal(zxid(), other.zxid())
                && Objects.equal(reply(), other.reply());
    }
}
