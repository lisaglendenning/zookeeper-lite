package edu.uw.zookeeper.protocol;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;

import edu.uw.zookeeper.util.TimeValue;

public abstract class OpPing implements Operation.Action, Operation.XidHeader {

    public static class Request extends OpPing implements Operation.Request {
        public static Request create() {
            return new Request();
        }

        private Request() {
            super();
        }
    }

    public static class Response extends OpPing implements
            Operation.Response {
        public static Response create() {
            return new Response();
        }

        private Response() {
            super();
        }
    }

    public static Records.OpCodeXid opCodeXid() {
        return Records.OpCodeXid.PING;
    }

    public static TimeValue now() {
        return TimeValue.create(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    private final TimeValue time;

    protected OpPing() {
        this(now());
    }

    protected OpPing(TimeValue time) {
        this.time = time;
    }
    
    @Override
    public OpCode opcode() {
        return opCodeXid().opcode();
    }

    @Override
    public int xid() {
        return opCodeXid().xid();
    }

    public TimeValue time() {
        return time;
    }

    public TimeValue difference(OpPing other) {
        return time().difference(other.time());
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("time", time()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(time());
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
        OpPing other = (OpPing) obj;
        return Objects.equal(time(), other.time());
    }
}
