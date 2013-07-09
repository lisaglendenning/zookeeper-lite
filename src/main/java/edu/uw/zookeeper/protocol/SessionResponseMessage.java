package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;

import edu.uw.zookeeper.protocol.proto.ByteBufInputArchive;
import edu.uw.zookeeper.protocol.proto.ByteBufOutputArchive;
import edu.uw.zookeeper.protocol.proto.IErrorResponse;
import edu.uw.zookeeper.protocol.proto.IReplyHeader;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.AbstractPair;


public class SessionResponseMessage extends AbstractPair<IReplyHeader, Records.Response> implements Message.ServerResponse {

    public static SessionResponseMessage newInstance(
            int xid,
            long zxid,
            Records.Response response) {
        KeeperException.Code code = (response instanceof Operation.Error) ? ((Operation.Error) response).error() : KeeperException.Code.OK;
        IReplyHeader header = Records.Responses.Headers.newInstance(xid, zxid, code);
        return new SessionResponseMessage(header, response);
    }

    public static SessionResponseMessage decode(final OpCode opcode, ByteBuf input)
            throws IOException {
        return decode(new Function<Integer, OpCode>() {
            @Override
            public OpCode apply(@Nullable Integer input) {
                return opcode;
            }}, input);
    }

    public static SessionResponseMessage decode(
            Function<Integer, OpCode> xidToOpCode, ByteBuf input)
            throws IOException {
        ByteBufInputArchive archive = new ByteBufInputArchive(input);
        IReplyHeader header = Records.Responses.Headers.deserialize(archive);
        int err = header.getErr();
        Records.Response response;
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
                response = Ping.Response.newInstance();
                break;
            default:
                response = Records.Responses.deserialize(opcode, archive);
                break;
            }
        } else {
            response = new IErrorResponse(err);
        }
        return new SessionResponseMessage(header, response);
    }
    
    protected SessionResponseMessage(IReplyHeader header, Records.Response response) {
        super(header, response);
    }
    
    @Override
    public long zxid() {
        return first.getZxid();
    }

    @Override
    public int xid() {
        return first.getXid();
    }

    @Override
    public Records.Response response() {
        return second;
    }

    @Override
    public void encode(ByteBuf output) throws IOException {
        ByteBufOutputArchive archive = new ByteBufOutputArchive(output);
        Records.Responses.Headers.serialize(first, archive);
        // don't serialize errors
        if (KeeperException.Code.OK.intValue() == first.getErr()) {
            Records.Responses.serialize(second, archive);
        }
    }
}
