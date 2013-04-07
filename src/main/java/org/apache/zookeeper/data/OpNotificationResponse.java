package org.apache.zookeeper.data;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.protocol.Decodable;
import org.apache.zookeeper.protocol.Encodable;
import org.apache.zookeeper.protocol.Records;
import org.apache.zookeeper.WatchedEvent;

import com.google.common.base.Objects;

public class OpNotificationResponse implements Operation.CallRequest,
        Operation.Response, Encodable, Decodable {

    protected WatchedEvent response;

    public static OpNotificationResponse create() {
        return new OpNotificationResponse();
    }

    public static OpNotificationResponse create(WatchedEvent response) {
        return new OpNotificationResponse(response);
    }

    public static Records.OperationXid opXid() {
        return Records.OperationXid.NOTIFICATION;
    }

    public static WatcherEvent createRecord() {
        return Records.Requests.<WatcherEvent> create(opXid().operation());
    }

    public OpNotificationResponse() {
        this(null);
    }

    public OpNotificationResponse(WatchedEvent response) {
        this.response = response;
    }

    @Override
    public Operation operation() {
        return opXid().operation();
    }

    @Override
    public int xid() {
        return opXid().xid();
    }

    public WatchedEvent event() {
        return response;
    }

    public OpNotificationResponse setResponse(WatchedEvent response) {
        this.response = response;
        return this;
    }

    @Override
    public Decodable decode(InputStream stream) throws IOException {
        WatcherEvent record = createRecord();
        Records.Responses.deserialize(record, stream);
        setResponse(new WatchedEvent(record));
        return this;
    }

    @Override
    public OutputStream encode(OutputStream stream) throws IOException {
        checkState(event() != null);
        return Records.Responses.serialize(event().getWrapper(), stream);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("response", event()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(event());
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
        OpNotificationResponse other = (OpNotificationResponse) obj;
        return Objects.equal(event(), other.event());
    }
}
