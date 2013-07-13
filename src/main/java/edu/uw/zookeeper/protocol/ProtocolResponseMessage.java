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


public class ProtocolResponseMessage<T extends Records.Response> extends AbstractPair<IReplyHeader, T> implements Message.ServerResponse<T> {

    public static <T extends Records.Response> ProtocolResponseMessage<T> of(
            int xid,
            long zxid,
            T response) {
        KeeperException.Code code = (response instanceof Operation.Error) ? ((Operation.Error) response).getError() : KeeperException.Code.OK;
        IReplyHeader header = Records.Responses.Headers.newInstance(xid, zxid, code);
        return of(header, response);
    }

    public static <T extends Records.Response> ProtocolResponseMessage<T> of(
            IReplyHeader header,
            T response) {
        return new ProtocolResponseMessage<T>(header, response);
    }

    public static ProtocolResponseMessage<?> decode(final OpCode opcode, ByteBuf input)
            throws IOException {
        return decode(new Function<Integer, OpCode>() {
            @Override
            public OpCode apply(@Nullable Integer input) {
                return opcode;
            }}, input);
    }

    public static ProtocolResponseMessage<?> decode(
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
                opcode = OpCodeXid.of(xid).getOpcode();
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
        return of(header, response);
    }
    
    protected ProtocolResponseMessage(IReplyHeader header, T response) {
        super(header, response);
    }
    
    @Override
    public long getZxid() {
        return first.getZxid();
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
        Records.Responses.Headers.serialize(first, archive);
        // don't serialize errors
        if (KeeperException.Code.OK.intValue() == first.getErr()) {
            Records.Responses.serialize(second, archive);
        }
    }
}
