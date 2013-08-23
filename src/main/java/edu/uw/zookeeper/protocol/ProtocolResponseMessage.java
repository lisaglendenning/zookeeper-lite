package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.protocol.proto.ByteBufInputArchive;
import edu.uw.zookeeper.protocol.proto.ByteBufOutputArchive;
import edu.uw.zookeeper.protocol.proto.IErrorResponse;
import edu.uw.zookeeper.protocol.proto.IReplyHeader;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;


public class ProtocolResponseMessage<T extends Records.Response> extends AbstractPair<IReplyHeader, T> implements Message.ServerResponse<T> {

    public static <T extends Records.Response> ProtocolResponseMessage<T> of(
            int xid,
            long zxid,
            T response) {
        KeeperException.Code code = (response instanceof Operation.Error) ? ((Operation.Error) response).error() : KeeperException.Code.OK;
        IReplyHeader header = Records.Responses.Headers.newInstance(xid, zxid, code);
        return of(header, response);
    }

    public static <T extends Records.Response> ProtocolResponseMessage<T> of(
            IReplyHeader header,
            T response) {
        return new ProtocolResponseMessage<T>(header, response);
    }

    public static ProtocolResponseMessage<?> decode(OpCode opcode, ByteBuf input)
            throws IOException {
        return decode(new ExpectedOpcode(opcode), input);
    }
    
    public static class ExpectedOpcode implements Function<Integer, OpCode> {

        public static ExpectedOpcode create(OpCode expected) {
            return new ExpectedOpcode(expected);
        }
        
        protected final OpCode expected;
        
        public ExpectedOpcode(OpCode expected) {
            this.expected = expected;
        }
        
        @Override
        public OpCode apply(Integer input) {
            return expected;
        }
    }
    
    public static void serialize(Operation.ProtocolResponse<?> value, OutputArchive archive) throws IOException {
        if (value instanceof ProtocolResponseMessage) {
            ((ProtocolResponseMessage<?>) value).serialize(archive);
        } else {
            Records.Response record = value.record();
            KeeperException.Code err;
            if (record instanceof Operation.Error) {
                err = ((Operation.Error) record).error();
            } else {
                err = KeeperException.Code.OK;
            }
            IReplyHeader header = Records.Responses.Headers.newInstance(value.xid(), value.zxid(), err);
            Records.Responses.Headers.serialize(header, archive);
            if (KeeperException.Code.OK.intValue() == header.getErr()) {
                Records.Responses.serialize(record, archive);
            }
        }
    }

    public static ProtocolResponseMessage<?> decode(
            Function<Integer, OpCode> xidToOpCode, ByteBuf input)
            throws IOException {
        return deserialize(xidToOpCode, new ByteBufInputArchive(input));
    }

    public static ProtocolResponseMessage<?> deserialize(
            Function<Integer, OpCode> xidToOpCode, InputArchive archive)
            throws IOException {
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
            case CREATE_SESSION:
                throw new IllegalArgumentException(String.valueOf(opcode));
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

    public IReplyHeader header() {
        return first;
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
    public T record() {
        return second;
    }

    @Override
    public void encode(ByteBuf output) throws IOException {
        ByteBufOutputArchive archive = new ByteBufOutputArchive(output);
        serialize(archive);
    }
    
    public void serialize(OutputArchive archive) throws IOException {
        Records.Responses.Headers.serialize(first, archive);
        // don't serialize errors
        if (KeeperException.Code.OK.intValue() == first.getErr()) {
            Records.Responses.serialize(second, archive);
        }
    }
}
