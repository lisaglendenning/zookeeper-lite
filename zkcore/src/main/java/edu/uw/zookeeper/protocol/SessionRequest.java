package edu.uw.zookeeper.protocol;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.proto.Records;

public class SessionRequest<T extends Records.Request> implements SessionOperation.Request<T> {

    public static <T extends Records.Request> SessionRequest<T> of(
            long sessionId, Operation.RequestId xid, Operation.RecordHolder<T> record) {
        return new SessionRequest<T>(sessionId, xid, record);
    }
    
    protected final long sessionId;
    protected final Operation.RequestId xid;
    protected final Operation.RecordHolder<T> record;
    
    public SessionRequest(
            long sessionId, Operation.RequestId xid, Operation.RecordHolder<T> record) {
        this.sessionId = sessionId;
        this.xid = xid;
        this.record = record;
    }
    
    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public int xid() {
        return xid.xid();
    }

    @Override
    public T record() {
        return record.record();
    }
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("sessionId", String.format("0x%08x", getSessionId()))
                .add("xid", xid())
                .add("record", record())
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof SessionRequest)) {
            return false;
        }
        SessionRequest<?> other = (SessionRequest<?>) obj;
        return Objects.equal(getSessionId(), other.getSessionId())
                && Objects.equal(record(), other.record())
                && Objects.equal(xid(), other.xid());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getSessionId(), record(), xid());
    }
}
