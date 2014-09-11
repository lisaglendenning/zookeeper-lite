package edu.uw.zookeeper.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.protocol.proto.Records;

public class SessionRequest<T extends Records.Request> implements SessionOperation.Request<T>, Reference<Message.ClientRequest<T>> {

    public static <T extends Records.Request> SessionRequest<T> of(
            long sessionId, Message.ClientRequest<T> request) {
        return new SessionRequest<T>(sessionId, request);
    }
    
    protected final long sessionId;
    protected final Message.ClientRequest<T> request;
    
    public SessionRequest(
            long sessionId, Message.ClientRequest<T> request) {
        this.sessionId = sessionId;
        this.request = request;
    }
        
    @Override
    public Message.ClientRequest<T> get() {
        return request;
    }

    @Override
    public long getSessionId() {
        return sessionId;
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
        return MoreObjects.toStringHelper(this)
                .add("sessionId", String.format("0x%08x", getSessionId()))
                .add("request", get())
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
                && Objects.equal(get(), other.get());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getSessionId(), get());
    }
}
