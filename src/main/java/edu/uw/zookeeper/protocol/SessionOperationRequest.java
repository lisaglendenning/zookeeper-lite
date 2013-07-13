package edu.uw.zookeeper.protocol;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.proto.Records;

public class SessionOperationRequest<T extends Records.Request> implements SessionOperation.Request<T> {

    public static <T extends Records.Request> SessionOperationRequest<T> of(
            long sessionId, Operation.RequestId xid, Operation.RecordHolder<T> record) {
        return new SessionOperationRequest<T>(sessionId, xid, record);
    }
    
    protected final long sessionId;
    protected final Operation.RequestId xid;
    protected final Operation.RecordHolder<T> record;
    
    public SessionOperationRequest(
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
    public int getXid() {
        return xid.getXid();
    }

    @Override
    public T getRecord() {
        return record.getRecord();
    }
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("sessionId", String.format("0x%08x", getSessionId()))
                .add("xid", getXid())
                .add("record", getRecord())
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof SessionOperationRequest)) {
            return false;
        }
        SessionOperationRequest<?> other = (SessionOperationRequest<?>) obj;
        return Objects.equal(getSessionId(), other.getSessionId())
                && Objects.equal(getRecord(), other.getRecord())
                && Objects.equal(getXid(), other.getXid());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getSessionId(), getRecord(), getXid());
    }
}
