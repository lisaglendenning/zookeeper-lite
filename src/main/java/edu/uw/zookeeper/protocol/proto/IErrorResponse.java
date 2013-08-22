package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.ErrorResponse;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.Operation;

@Operational(value=OpCode.ERROR)
public class IErrorResponse extends ICodedRecord<ErrorResponse> implements Records.Response, Records.MultiOpResponse, Operation.Error {
    public IErrorResponse() {
        this(new ErrorResponse());
    }
    
    public IErrorResponse(KeeperException.Code err) {
        this(err.intValue());
    }

    public IErrorResponse(int err) {
        this(new ErrorResponse(err));
    }

    public IErrorResponse(ErrorResponse record) {
        super(record);
    }

    @Override
    public KeeperException.Code error() {
        return KeeperException.Code.get(getErr());
    }
    
    public int getErr() {
        return record.getErr();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(error()).toString();
    }
}
