package org.apache.zookeeper.data;

import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.protocol.Records;
import org.apache.zookeeper.protocol.Records.OperationXid;

import com.google.common.base.Objects;

public abstract class OpPingAction extends OpXid {

    public static class Request extends OpPingAction implements
            Operation.Request {
        public static Request create() {
            return new Request();
        }

        public Request() {
            super();
        }
    }

    public static class Response extends OpPingAction implements
            Operation.Response {
        public static Response create() {
            return new Response();
        }

        public Response() {
            super();
        }
    }

    protected long timestamp;
    protected TimeUnit timeUnit;

    protected OpPingAction() {
        this(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    protected OpPingAction(long timestamp, TimeUnit timeUnit) {
        this.timestamp = timestamp;
        this.timeUnit = timeUnit;
    }

    @Override
    public Records.OperationXid opXid() {
        return Records.OperationXid.PING;
    }

    public long timestamp() {
        return timestamp;
    }

    public OpPingAction setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public TimeUnit timeUnit() {
        return timeUnit;
    }

    public OpPingAction setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
        return this;
    }

    public long difference(OpPingAction other) {
        return timestamp()
                - timeUnit.convert(other.timestamp(), other.timeUnit());
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("timestamp", timestamp())
                .add("timeUnit", timeUnit()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(timestamp(), timeUnit());
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
        OpPingAction other = (OpPingAction) obj;
        return Objects.equal(timestamp(), other.timestamp())
                && Objects.equal(timeUnit(), other.timeUnit());
    }
}
