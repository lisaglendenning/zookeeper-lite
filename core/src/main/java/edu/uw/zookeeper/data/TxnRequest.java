package edu.uw.zookeeper.data;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;


public class TxnRequest<T extends Records.Request> implements TxnOperation.Request<T> {

    public static <T extends Records.Request> TxnRequest<T> of(long time, long zxid, SessionOperation.Request<T> request) {
        return new TxnRequest<T>(time, zxid, request);
    }
    
    protected final long time;
    protected final long zxid;
    protected final SessionOperation.Request<T> request;
    
    public TxnRequest(long time, long zxid, SessionOperation.Request<T> request) {
        this.time = time;
        this.zxid = zxid;
        this.request = request;
    }

    @Override
    public long getTime() {
        return time;
    }

    @Override
    public long zxid() {
        return zxid;
    }

    @Override
    public long getSessionId() {
        return request.getSessionId();
    }

    @Override
    public int xid() {
        return request.xid();
    }

    @Override
    public T record() {
        return request.record();
    }
    
    @Override
    public String toString() {
        return Records.toBeanString(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof TxnOperation.Request)) {
            return false;
        }
        TxnRequest<?> other = (TxnRequest<?>) obj;
        return Objects.equal(getSessionId(), other.getSessionId())
                && Objects.equal(zxid(), other.zxid())
                && Objects.equal(getTime(), other.getTime())
                && Objects.equal(record(), other.record())
                && Objects.equal(xid(), other.xid());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getSessionId(), zxid(), getTime(), record(), xid());
    }
}
