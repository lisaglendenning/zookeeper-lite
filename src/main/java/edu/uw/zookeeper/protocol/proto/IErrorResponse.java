package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.ErrorResponse;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public class IErrorResponse extends ErrorResponse implements Records.MultiOpResponse, Operation.Error {
    public static final OpCode OPCODE = OpCode.ERROR;
    
    public IErrorResponse() {
        super();
    }
    
    public IErrorResponse(KeeperException.Code err) {
        this(err.intValue());
    }

    public IErrorResponse(int err) {
        super(err);
    }

    @Override
    public KeeperException.Code error() {
        return KeeperException.Code.get(getErr());
    }

    @Override
    public OpCode opcode() {
        return OPCODE;
    }

    @Override
    public void serialize(OutputArchive archive) throws IOException {
        serialize(archive, Records.Responses.TAG);
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Records.Responses.TAG);
    }
}
