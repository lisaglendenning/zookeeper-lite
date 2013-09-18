package edu.uw.zookeeper.protocol;

import java.util.concurrent.TimeUnit;

import org.apache.jute.Record;

import com.google.common.base.Objects;

import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.proto.IOpCodeXidRecord;
import edu.uw.zookeeper.protocol.proto.IPingRequest;
import edu.uw.zookeeper.protocol.proto.IPingResponse;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Operational;
import edu.uw.zookeeper.protocol.proto.OperationalXid;
import edu.uw.zookeeper.protocol.proto.Records;

@Operational(value=OpCode.PING)
@OperationalXid(value=OpCodeXid.PING)
public abstract class Ping<T extends Record> extends IOpCodeXidRecord<T> {

    public static OpCodeXid OPCODE_XID = OpCodeXid.PING;
    
    public static TimeValue now() {
        return TimeValue.create(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }
    
    protected final TimeValue time;

    protected Ping(T record) {
        this(record, now());
    }

    protected Ping(T record, TimeValue time) {
        super(record);
        this.time = time;
    }

    public TimeValue time() {
        return time;
    }

    public TimeValue difference(Ping<?> other) {
        return time.difference(other.time);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("record", record)
                .add("time", time).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(record, time);
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
        Ping<?> other = (Ping<?>) obj;
        return Objects.equal(record, other.record) 
                && Objects.equal(time, other.time);
    }
    
    public static class Request extends Ping<IPingRequest> implements 
            Records.Request {

        public static IPingRequest getRecord() {
            return (IPingRequest) Records.Requests.getInstance().get(OpCode.PING);
        }
        
        public static Ping.Request newInstance() {
            return new Ping.Request();
        }

        private Request() {
            super(getRecord());
        }
    }

    public static class Response extends Ping<IPingResponse> implements
            Records.Response {
        
        public static IPingResponse getRecord() {
            return (IPingResponse) Records.Responses.getInstance().get(OpCode.PING);
        }
        
        public static Ping.Response newInstance() {
            return new Ping.Response();
        }

        private Response() {
            super(getRecord());
        }
    }
}