package edu.uw.zookeeper.protocol;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.proto.IPingRequest;
import edu.uw.zookeeper.protocol.proto.IPingResponse;
import edu.uw.zookeeper.util.TimeValue;

public abstract class OpPing {

    public static Records.OpCodeXid OPCODE_XID = Records.OpCodeXid.PING;
    
    public static TimeValue now() {
        return TimeValue.create(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }
    
    public interface Timestamped {
        TimeValue asTime();
    }

    public static class OpPingRequest extends OpRecord.OpRequest<IPingRequest> implements Operation.Request, Operation.XidHeader, Timestamped {

        public static IPingRequest newRecord() {
            return (IPingRequest) Records.Requests.getInstance().get(IPingRequest.OPCODE);
        }
        
        public static OpPingRequest newInstance() {
            return newInstance(newRecord());
        }

        public static OpPingRequest newInstance(IPingRequest record) {
            return new OpPingRequest(record, now());
        }

        private final TimeValue time;

        private OpPingRequest(IPingRequest record, TimeValue time) {
            super(record);
            this.time = time;
        }
        
        @Override
        public int xid() {
            return asRecord().xid();
        }

        @Override
        public TimeValue asTime() {
            return time;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("time", asTime()).toString();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(asTime());
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
            OpPingRequest other = (OpPingRequest) obj;
            return Objects.equal(asTime(), other.asTime());
        }
    }

    public static class OpPingResponse extends OpRecord.OpResponse<IPingResponse> implements
            Operation.Response, Operation.XidHeader, Timestamped {
        
        public static IPingResponse newRecord() {
            return (IPingResponse) Records.Responses.getInstance().get(IPingResponse.OPCODE);
        }
        
        public static OpPingResponse newInstance() {
            return newInstance(newRecord());
        }

        public static OpPingResponse newInstance(IPingResponse record) {
            return new OpPingResponse(record, now());
        }

        private final TimeValue time;

        private OpPingResponse(IPingResponse record, TimeValue time) {
            super(record);
            this.time = time;
        }
        
        public TimeValue difference(OpPingRequest request) {
            return asTime().difference(request.asTime());
        }

        @Override
        public int xid() {
            return asRecord().xid();
        }

        @Override
        public TimeValue asTime() {
            return time;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("time", asTime()).toString();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(asTime());
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
            OpPingResponse other = (OpPingResponse) obj;
            return Objects.equal(asTime(), other.asTime());
        }
    }
}
