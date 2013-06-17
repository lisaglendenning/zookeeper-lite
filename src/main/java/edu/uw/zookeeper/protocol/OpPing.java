package edu.uw.zookeeper.protocol;

import java.util.concurrent.TimeUnit;

import org.apache.jute.Record;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.proto.IOperationalXidRecord;
import edu.uw.zookeeper.protocol.proto.IPingRequest;
import edu.uw.zookeeper.protocol.proto.IPingResponse;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.OperationalXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.TimeValue;

@OperationalXid(xid=OpCodeXid.PING)
public abstract class OpPing<T extends Record> extends IOperationalXidRecord<T> {

    public static OpCodeXid OPCODE_XID = OpCodeXid.PING;
    
    public static TimeValue now() {
        return TimeValue.create(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }
    
    protected final TimeValue time;

    protected OpPing(T record) {
        this(record, now());
    }

    protected OpPing(T record, TimeValue time) {
        super(record);
        this.time = time;
    }

    public TimeValue getTime() {
        return time;
    }

    public TimeValue difference(OpPing<?> other) {
        return getTime().difference(other.getTime());
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("record", get())
                .add("time", getTime()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(get(), getTime());
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
        OpPing<?> other = (OpPing<?>) obj;
        return Objects.equal(get(), other.get()) 
                && Objects.equal(getTime(), other.getTime());
    }
    
    public static class Request extends OpPing<IPingRequest> implements Operation.Request {

        public static IPingRequest getRecord() {
            return (IPingRequest) Records.Requests.getInstance().get(OpCode.PING);
        }
        
        public static Request newInstance() {
            return new Request();
        }

        private Request() {
            super(getRecord());
        }
    }

    public static class Response extends OpPing<IPingResponse> implements
            Operation.Response {
        
        public static IPingResponse getRecord() {
            return (IPingResponse) Records.Responses.getInstance().get(OpCode.PING);
        }
        
        public static Response newInstance() {
            return new Response();
        }

        private Response() {
            super(getRecord());
        }
    }
}
