package org.apache.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.proto.*;

import com.google.common.base.Objects;
import com.google.common.collect.BiMap;
import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.Maps;

public class Records {

    // Used by ConnectRequest/Response
    public static final int PROTOCOL_VERSION = 0;

    // Used by ConnectRequest/Response
    public static final String CONNECT_TAG = "connect";

    // Used by Notify
    public static final String NOTIFICATION_TAG = "notification";

    // Doesn't seem to be used consistently, but used for encoding frame length
    public static final String LEN_TAG = "len";

    // These are hardcoded in various places in zookeeper code...
    public static enum OperationXid {
        // response only?
        NOTIFICATION(-1, Operation.NOTIFICATION), // zxid is -1?
        // request and response
        PING(-2, Operation.PING), // zxid is lastZxid
        // request and response
        AUTH(-4, Operation.AUTH), // zxid is 0
        // request and response
        SET_WATCHES(-8, Operation.SET_WATCHES); // zxid is lastZxid

        protected static Map<Integer, OperationXid> xidToType = Maps
                .newHashMap();
        static {
            for (OperationXid item : OperationXid.values()) {
                Integer xid = item.xid();
                assert (!(xidToType.containsKey(xid)));
                xidToType.put(xid, item);
            }
        }

        protected final int xid;
        protected final Operation operation;

        OperationXid(int xid, Operation operation) {
            this.xid = xid;
            this.operation = operation;
        }

        public int xid() {
            return xid;
        }

        public Operation operation() {
            return operation;
        }

        public static boolean has(int xid) {
            return xidToType.containsKey(xid);
        }

        public static OperationXid get(int xid) {
            checkArgument(xidToType.containsKey(xid));
            return xidToType.get(xid);
        }
    }

    protected static BiMap<Operation, Class<? extends Record>> requestTypes = EnumHashBiMap
            .create(Operation.class);
    static {
        requestTypes.put(Operation.CREATE, CreateRequest.class);
        requestTypes.put(Operation.DELETE, DeleteRequest.class);
        requestTypes.put(Operation.EXISTS, ExistsRequest.class);
        requestTypes.put(Operation.GET_DATA, GetDataRequest.class);
        requestTypes.put(Operation.SET_DATA, SetDataRequest.class);
        requestTypes.put(Operation.GET_ACL, GetACLRequest.class);
        requestTypes.put(Operation.SET_ACL, SetACLRequest.class);
        requestTypes.put(Operation.GET_CHILDREN, GetChildrenRequest.class);
        requestTypes.put(Operation.SYNC, SyncRequest.class);
        requestTypes.put(Operation.GET_CHILDREN2, GetChildren2Request.class);
        requestTypes.put(Operation.CHECK, CheckVersionRequest.class);
        requestTypes.put(Operation.MULTI, MultiTransactionRecord.class);
        requestTypes.put(Operation.AUTH, AuthPacket.class);
        requestTypes.put(Operation.SET_WATCHES, SetWatches.class);
        requestTypes.put(Operation.CREATE_SESSION, ConnectRequest.class);
    }

    protected static BiMap<Operation, Class<? extends Record>> responseTypes = EnumHashBiMap
            .create(Operation.class);
    static {
        responseTypes.put(Operation.CREATE, CreateResponse.class);
        responseTypes.put(Operation.EXISTS, ExistsResponse.class);
        responseTypes.put(Operation.GET_DATA, GetDataResponse.class);
        responseTypes.put(Operation.SET_DATA, SetDataResponse.class);
        responseTypes.put(Operation.GET_ACL, GetACLResponse.class);
        responseTypes.put(Operation.SET_ACL, SetACLResponse.class);
        responseTypes.put(Operation.GET_CHILDREN, GetChildrenResponse.class);
        responseTypes.put(Operation.SYNC, SyncResponse.class);
        responseTypes.put(Operation.GET_CHILDREN2, GetChildren2Response.class);
        responseTypes.put(Operation.MULTI, MultiResponse.class);
        responseTypes.put(Operation.CREATE_SESSION, ConnectResponse.class);
        responseTypes.put(Operation.NOTIFICATION, WatcherEvent.class);
    }

    public static class Headers {

        // Used for RequestHeader/ReplyHeader
        public static final String TAG = "header";

        public static boolean contains(Class<? extends Record> recordType) {
            return Requests.Headers.contains(recordType)
                    || Responses.Headers.contains(recordType);
        }

        public static RequestHeader deserialize(RequestHeader header,
                InputStream stream) throws IOException {
            return Records.deserialize(header, stream, TAG);
        }

        public static ReplyHeader deserialize(ReplyHeader header,
                InputStream stream) throws IOException {
            return Records.deserialize(header, stream, TAG);
        }

        public static OutputStream serialize(RequestHeader header,
                OutputStream stream) throws IOException {
            return Records.serialize(header, stream, TAG);
        }

        public static OutputStream serialize(ReplyHeader header,
                OutputStream stream) throws IOException {
            return Records.serialize(header, stream, TAG);
        }
    }

    public static class Requests {

        // Used for Requests
        public static final String TAG = "request";

        public static class Headers {
            public static Class<RequestHeader> getType() {
                return RequestHeader.class;
            }

            public static boolean contains(Class<? extends Record> recordType) {
                return getType().isAssignableFrom(recordType);
            }

            public static RequestHeader create() {
                return new RequestHeader();
            }

            public static RequestHeader create(int xid, int type) {
                return new RequestHeader(xid, type);
            }

            public static RequestHeader create(int xid, Operation op) {
                return create(xid, op.code());
            }

            public static RequestHeader deserialize(InputStream stream)
                    throws IOException {
                return Records.deserialize(create(), stream, TAG);
            }

            public static OutputStream serialize(int xid, int type,
                    OutputStream stream) throws IOException {
                return Records.Headers.serialize(create(xid, type), stream);
            }

            public static OutputStream serialize(int xid, Operation op,
                    OutputStream stream) throws IOException {
                return Records.Headers.serialize(create(xid, op), stream);
            }
        }

        public static Class<? extends Record> getType(Operation op) {
            return requestTypes.get(op);
        }

        public static Operation recordToOperation(Record record) {
            return recordToOperation(record.getClass());
        }

        public static Operation recordToOperation(
                Class<? extends Record> recordType) {
            Map<Class<? extends Record>, Operation> typeMap = requestTypes
                    .inverse();
            Operation operation = typeMap.get(recordType);
            return operation;
        }

        public static boolean contains(Class<? extends Record> recordType) {
            Map<Class<? extends Record>, Operation> typeMap = requestTypes
                    .inverse();
            return typeMap.containsKey(recordType);
        }

        @SuppressWarnings("unchecked")
        public static <T extends Record> T create(Operation op, Object... args) {
            Class<? extends Record> recordType = getType(op);
            if (recordType == null) {
                throw new UnsupportedOperationException();
            }
            T record;
            try {
                record = (T) Records.newInstance(recordType, args);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
            return record;
        }

        public static <T extends Record> T deserialize(T request,
                InputStream stream) throws IOException {
            return Records.deserialize(request, stream, TAG);
        }

        @SuppressWarnings("unchecked")
        public static <T extends Record> T deserialize(Operation op,
                InputStream stream) throws IOException {
            return deserialize((T) create(op), stream);
        }

        public static OutputStream serialize(Record request, OutputStream stream)
                throws IOException {
            return Records.serialize(request, stream, TAG);
        }
    }

    public static class Responses {

        // Used for Responses
        public static final String TAG = "response";

        public static class Headers {
            public static Class<ReplyHeader> getType() {
                return ReplyHeader.class;
            }

            public static boolean contains(Class<? extends Record> recordType) {
                return getType().isAssignableFrom(recordType);
            }

            public static ReplyHeader create() {
                return new ReplyHeader();
            }

            public static ReplyHeader create(int xid, long zxid, int err) {
                return new ReplyHeader(xid, zxid, err);
            }

            public static ReplyHeader create(int xid, long zxid,
                    KeeperException.Code code) {
                return create(xid, zxid, code.intValue());
            }

            public static ReplyHeader deserialize(InputStream stream)
                    throws IOException {
                return Records.Headers.deserialize(create(), stream);
            }

            public static OutputStream serialize(int xid, long zxid, int err,
                    OutputStream stream) throws IOException {
                return Records.Headers
                        .serialize(create(xid, zxid, err), stream);
            }

            public static OutputStream serialize(int xid, long zxid,
                    KeeperException.Code code, OutputStream stream)
                    throws IOException {
                return Records.Headers.serialize(create(xid, zxid, code),
                        stream);
            }
        }

        public static Class<? extends Record> getType(Operation op) {
            return responseTypes.get(op);
        }

        public static Operation recordToOperation(Record record) {
            return recordToOperation(record.getClass());
        }

        public static Operation recordToOperation(
                Class<? extends Record> recordType) {
            Map<Class<? extends Record>, Operation> typeMap = responseTypes
                    .inverse();
            Operation operation = typeMap.get(recordType);
            return operation;
        }

        public static boolean contains(Class<? extends Record> recordType) {
            Map<Class<? extends Record>, Operation> typeMap = responseTypes
                    .inverse();
            return typeMap.containsKey(recordType);
        }

        @SuppressWarnings("unchecked")
        public static <T extends Record> T create(Operation op, Object... args) {
            Class<? extends Record> recordType = getType(op);
            if (recordType == null) {
                throw new UnsupportedOperationException();
            }
            T record;
            try {
                record = (T) Records.newInstance(recordType, args);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
            return record;
        }

        public static <T extends Record> T deserialize(T response,
                InputStream stream) throws IOException {
            return Records.deserialize(response, stream, TAG);
        }

        @SuppressWarnings("unchecked")
        public static <T extends Record> T deserialize(Operation op,
                InputStream stream) throws IOException {
            return deserialize((T) create(op), stream);
        }

        public static OutputStream serialize(Record response,
                OutputStream stream) throws IOException {
            return Records.serialize(response, stream, TAG);
        }
    }

    public static Operation recordToOperation(Record record) {
        return recordToOperation(record.getClass());
    }

    public static Operation recordToOperation(Class<? extends Record> recordType) {
        Operation operation = Requests.recordToOperation(recordType);
        if (operation == null) {
            operation = Responses.recordToOperation(recordType);
        }
        return operation;
    }

    public static String recordToTag(Record record) {
        return recordToTag(record.getClass());
    }

    public static String recordToTag(Class<? extends Record> recordType) {
        String tag = null;
        if (Headers.contains(recordType)) {
            tag = Headers.TAG;
        } else if ((ConnectRequest.class.isAssignableFrom(recordType))
                || (ConnectResponse.class.isAssignableFrom(recordType))) {
            tag = CONNECT_TAG;
        } else if (Requests.contains(recordType)) {
            tag = Requests.TAG;
        } else if (Responses.contains(recordType)) {
            tag = Responses.TAG;
        }
        return tag;
    }

    public static <T extends Record> T deserialize(T record, InputStream stream)
            throws IOException {
        return deserialize(record, stream, recordToTag(record));
    }

    public static <T extends Record> T deserialize(T record,
            InputStream stream, String tag) throws IOException {
        BinaryInputArchive bia = BinaryInputArchive.getArchive(stream);
        record.deserialize(bia, tag);
        return record;
    }

    public static <T extends Record> OutputStream serialize(T record,
            OutputStream stream) throws IOException {
        return serialize(record, stream, recordToTag(record));
    }

    public static <T extends Record> OutputStream serialize(T record,
            OutputStream stream, String tag) throws IOException {
        BinaryOutputArchive bos = BinaryOutputArchive.getArchive(stream);
        record.serialize(bos, tag);
        return stream;
    }

    /*
     * More readable String
     */
    public static String toString(Record record) {
        return Objects.toStringHelper(record)
                .addValue(record.toString().replaceAll("\\s", "")).toString();
    }

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<T> cls, Object... args)
            throws IllegalArgumentException, InstantiationException,
            IllegalAccessException, InvocationTargetException {
        Constructor<T> ctor = null;
        for (Constructor<?> candidate : cls.getConstructors()) {
            if (candidate.getParameterTypes().length == args.length) {
                ctor = (Constructor<T>) candidate;
                break;
            }
        }
        assert ctor != null;
        T instance = ctor.newInstance(args);
        return instance;
    }

}
