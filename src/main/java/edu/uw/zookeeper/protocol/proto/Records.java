package edu.uw.zookeeper.protocol.proto;


import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collections;

import javax.annotation.Nullable;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.BiMap;
import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.MutableClassToInstanceMap;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.ParameterizedFactory;


/**
 * Utility functions for <code>Record</code> objects and types.
 * 
 * @see org.apache.zookeeper.proto
 */
public abstract class Records {
   
    public static interface HeaderRecord extends Record {}
    
    public static interface MultiOpRequest extends Operation.Request {}
    public static interface MultiOpResponse extends Operation.Response {}

    public static interface ConnectHolder {
        int getProtocolVersion();
        int getTimeOut();
        long getSessionId();
        byte[] getPasswd();
    }
    
    public static interface ConnectRecord extends ConnectHolder {
        void setProtocolVersion(int version);
        void setTimeOut(int timeOut);
        void setSessionId(long sessionId);
        void setPasswd(byte[] passwd);
    }
    
    public static interface CreateStatHolder {
        long getCzxid();
        long getCtime();
        long getEphemeralOwner();
    }

    public static interface CreateStatRecord extends CreateStatHolder {
        void setCzxid(long czxid);
        void setCtime(long ctime);
        void setEphemeralOwner(long ephemeralOwner);
    }

    public static interface DataStatHolder {
        long getMzxid();
        long getMtime();
        int getVersion();
    }

    public static interface DataStatRecord extends DataStatHolder {
        void setMzxid(long mzxid);
        void setMtime(long mtime);
        void setVersion(int version);
    }
    
    public static interface AclStatHolder {
        int getAversion();
    }
    
    public static interface AclStatRecord extends AclStatHolder {
        void setAversion(int aversion);
    }

    public static interface ChildrenStatHolder {
        int getCversion();
        long getPzxid();
    }
    
    public static interface ChildrenStatRecord extends ChildrenStatHolder {
        // pzxid seems to be the zxid related to cversion
        void setCversion(int cversion);
        void setPzxid(long pzxid);
    }
    
    public static interface StatPersistedHolder extends CreateStatHolder, DataStatHolder, AclStatHolder, ChildrenStatHolder {
    }

    public static interface StatPersistedRecord extends StatPersistedHolder, CreateStatRecord, DataStatRecord, AclStatRecord, ChildrenStatRecord {
    }
    
    public static interface StatHolderInterface extends StatPersistedHolder {
        int getDataLength();
        int getNumChildren();
    }

    public static interface StatRecordInterface extends StatHolderInterface, StatPersistedRecord {
        void setDataLength(int dataLength);
        void setNumChildren(int numChildren);
    }

    public static interface View {}
    
    public static interface PathHolder extends View {
        String getPath();
    }
    
    public static interface PathRecord extends PathHolder {
        void setPath(String path);
    }
    
    public static interface StatHolder extends View {
        Stat getStat();
    }
    
    public static interface StatRecord extends StatHolder {
        void setStat(Stat stat);
    }
    
    public static interface DataHolder extends View {
        byte[] getData();    
    }

    public static interface DataRecord extends DataHolder {
        void setData(byte[] data);     
    }
    
    public static interface AclHolder extends View {
        java.util.List<org.apache.zookeeper.data.ACL> getAcl();     
    }

    public static interface AclRecord extends AclHolder {
        void setAcl(java.util.List<org.apache.zookeeper.data.ACL> acl);       
    }
    
    public static interface ChildrenHolder extends View {
        java.util.List<String> getChildren();
    }

    public static interface ChildrenRecord extends ChildrenHolder {
        void setChildren(java.util.List<String> children);
    }
    
    public static interface VersionHolder extends View {
        int getVersion();
    }
    
    public static interface VersionRecord extends VersionHolder {
        void setVersion(int version);
    }

    public static interface WatchHolder {
        boolean getWatch();
    }

    public static interface WatchRecord extends WatchHolder {
        void setWatch(boolean watch);
    }

    public static interface CreateHolder extends PathHolder, DataHolder, AclHolder {
        int getFlags();
    }
    
    public static interface CreateRecord extends CreateHolder, PathRecord, DataRecord, AclRecord {
        void setFlags(int flags);
    }
    
    // Used by ConnectRequest/Response
    public static final int PROTOCOL_VERSION = 0;

    // Used by ConnectRequest/Response
    public static final String CONNECT_TAG = "connect";

    // Used by Notify
    public static final String NOTIFICATION_TAG = "notification";

    // Doesn't seem to be used consistently, but used for encoding lengths
    public static final String LEN_TAG = "len";
    
    public static OpCode opCodeOf(Class<?> type) {
        Operational operational = type.getAnnotation(Operational.class);
        if (operational == null) {
            OpCodeXid xid = opCodeXidOf(type);
            if (xid != null) {
                return xid.opcode();
            } else {
                return null;
            }
        } else {
            return operational.opcode();
        }
    }
    
    public static OpCodeXid opCodeXidOf(Class<?> type) {
        OperationalXid operationalXid = type.getAnnotation(OperationalXid.class);
        if (operationalXid != null) {
            return operationalXid.xid();
        } else {
            return null;
        }
    }

    public static <T> T create(Class<T> type) {
        T instance;
        if (type.getAnnotation(Shared.class) != null) {
            return getShared(type);
        } else {
            try {
                instance = type.getConstructor().newInstance();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
        return instance;
    }
    
    public static <T> T getShared(Class<T> type) {
        T instance = sharedInstances.getInstance(type);
        if (instance == null) {
            synchronized (sharedInstances) {
                instance = sharedInstances.getInstance(type);
                if (instance == null) {
                    if (type.getAnnotation(Shared.class) == null) {
                        throw new IllegalArgumentException(type.toString());
                    }
                    try {
                        instance = type.getConstructor().newInstance();
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    sharedInstances.put(type, instance);
                }
            }
        }
        return instance;
    }

    /**
     * Default string value without endlines.
     */
    public static String toString(Record record) {
        return Objects.toStringHelper(record)
                .addValue(record.toString().replaceAll("\\s", "")).toString();
    }

    public static enum ToBeanString implements Function<Object, String> {
        INSTANCE;
        
        @Override
        public @Nullable String apply(@Nullable Object input) {
            if (input instanceof Record) {
                return toBeanString((Record) input);
            } else {
                return String.valueOf(input);
            }
        }
    }
    
    /**
     * Bean property string.
     */
    public static String toBeanString(Record record) {
        Objects.ToStringHelper helper = Objects.toStringHelper(record);
        BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(record.getClass(), Object.class);
        } catch (IntrospectionException e) {
            throw new AssertionError(e);
        }
        for (PropertyDescriptor pd : beanInfo.getPropertyDescriptors()) {
            Object value;
            try {
                value = pd.getReadMethod().invoke(record);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            if (value instanceof Record) {
                value = toBeanString((Record) value);
            } else if (value.getClass().isArray()) {
                value = String.format("%s{%d}", value.getClass().getComponentType(), Array.getLength(value));
            } else if (value instanceof Iterable) {
                value = iterableToBeanString((Iterable<?>) value);
            }
            helper.add(pd.getName(), value);
        }
        return helper.toString();
    }
    
    public static String iterableToBeanString(Iterable<?> value) {
        StringBuilder sb = new StringBuilder().append('[');
        return Joiner.on(',').appendTo(
                sb,
                Iterators.transform(
                        value.iterator(), 
                        ToBeanString.INSTANCE))
                .append(']').toString();
    }
    
    @SuppressWarnings("unchecked")
    private static final BiMap<OpCode, Class<? extends Operation.Request>> requestTypes = 
            Maps.unmodifiableBiMap(EnumHashBiMap.create(
                    Maps.<OpCode, Class<? extends Operation.Request>>uniqueIndex(
                            ImmutableList.<Class<? extends Operation.Request>>of(
                                    ICreateRequest.class,
                                    ICreate2Request.class,
                                    IDeleteRequest.class,
                                    IExistsRequest.class,
                                    IGetDataRequest.class,
                                    ISetDataRequest.class,
                                    IGetACLRequest.class,
                                    ISetACLRequest.class,
                                    IGetChildrenRequest.class,
                                    ISyncRequest.class,
                                    IPingRequest.class,
                                    IGetChildren2Request.class,
                                    ICheckVersionRequest.class,
                                    IAuthRequest.class,
                                    ISetWatchesRequest.class,
                                    IConnectRequest.class,
                                    IDisconnectRequest.class,
                                    IMultiRequest.class,
                                    IReconfigRequest.class),
                            new Function<Class<?>, OpCode>() {
                               @Override public OpCode apply(Class<?> type) {
                                   return opCodeOf(type);
                               }
                            })));

    @SuppressWarnings("unchecked")
    private static final BiMap<OpCode, Class<? extends Operation.Response>> responseTypes = 
            Maps.unmodifiableBiMap(EnumHashBiMap.create(
                    Maps.<OpCode, Class<? extends Operation.Response>>uniqueIndex(
                            ImmutableList.<Class<? extends Operation.Response>>of(
                                    IAuthResponse.class,
                                    ICreateResponse.class,
                                    ICreate2Response.class,
                                    IDeleteResponse.class,
                                    IDisconnectResponse.class,
                                    IExistsResponse.class,
                                    IGetDataResponse.class,
                                    ISetDataResponse.class,
                                    IGetACLResponse.class,
                                    ISetACLResponse.class,
                                    IGetChildrenResponse.class,
                                    ISyncResponse.class,
                                    IGetChildren2Response.class,
                                    IPingResponse.class,
                                    IConnectResponse.class,
                                    IWatcherEvent.class,
                                    ISetWatchesResponse.class,
                                    IErrorResponse.class,
                                    IMultiResponse.class),
                            new Function<Class<?>, OpCode>() {
                                @Override public OpCode apply(Class<?> type) {
                                    return opCodeOf(type);
                                }
                             })));
    
    private static final ClassToInstanceMap<Object> sharedInstances = 
            MutableClassToInstanceMap.create(Collections.synchronizedMap(Maps.<Class<? extends Object>, Object>newHashMap()));

    public static class Headers {

        private Headers() {}
        
        // Used for RequestHeader/ReplyHeader
        public static final String TAG = "header";
    }

    public static enum Requests implements ParameterizedFactory<OpCode, Operation.Request> {
        INSTANCE;

        public static Requests getInstance() {
            return INSTANCE;
        }
        
        public static String tagOf(OpCode op) {
            String tag;
            switch (op) {
            case CREATE_SESSION:
                tag = CONNECT_TAG;
                break;
            default:
                tag = Requests.TAG;
                break;
            }
            return tag;
        }
        
        public static final String TAG = "request";

        public static class Headers {
            
            public static Class<IRequestHeader> typeOf() {
                return IRequestHeader.class;
            }

            public static IRequestHeader create(int xid, OpCode op) {
                return new IRequestHeader(xid, op.intValue());
            }

            public static IRequestHeader deserialize(InputArchive archive)
                    throws IOException {
                IRequestHeader record = new IRequestHeader();
                record.deserialize(archive, Records.Headers.TAG);
                return record;
            }

            public static void serialize(int xid, OpCode op,
                    OutputArchive archive) throws IOException {
                serialize(create(xid, op), archive);
            }

            public static void serialize(IRequestHeader record, OutputArchive archive)
                    throws IOException {
                record.serialize(archive, Records.Headers.TAG);
            }
        }

        public static Class<? extends Operation.Request> typeOf(OpCode opcode) {
            return requestTypes.get(opcode);
        }

        public static Operation.Request deserialize(OpCode op,
                InputArchive archive) throws IOException {
            Operation.Request instance = getInstance().get(op);
            instance.deserialize(archive, tagOf(op));
            return instance;
        }

        public static void serialize(Operation.Request record, OutputArchive archive)
                throws IOException {
            record.serialize(archive, tagOf(record.opcode()));
        }
        
        @Override
        public Operation.Request get(OpCode opcode) {
            Class<? extends Operation.Request> type = typeOf(opcode);
            if (type == null) {
                throw new IllegalArgumentException(
                        String.format("No type for %s", opcode));
            }
            Operation.Request record = Records.create(type);
            return record;
        }
    }

    public static enum Responses implements ParameterizedFactory<OpCode, Operation.Response> {
        INSTANCE;
        
        public static Responses getInstance() {
            return INSTANCE;
        }

        public static String tagOf(OpCode op) {
            String tag;
            switch (op) {
            case CREATE_SESSION:
                tag = CONNECT_TAG;
                break;
            case NOTIFICATION: 
                tag = NOTIFICATION_TAG;
            default:
                tag = Responses.TAG;
                break;
            }
            return tag;
        }
        
        public static final String TAG = "response";

        public static class Headers {
            public static Class<IReplyHeader> typeOf() {
                return IReplyHeader.class;
            }
        
            public static IReplyHeader create(int xid, long zxid,
                    KeeperException.Code code) {
                return new IReplyHeader(xid, zxid, code.intValue());
            }

            public static IReplyHeader deserialize(InputArchive archive)
                    throws IOException {
                IReplyHeader record = new IReplyHeader();
                record.deserialize(archive, Records.Headers.TAG);
                return record;
            }

            public static void serialize(int xid, long zxid,
                    KeeperException.Code code,
                    OutputArchive archive) throws IOException {
                serialize(create(xid, zxid, code), archive);
            }

            public static void serialize(IReplyHeader record, OutputArchive archive)
                    throws IOException {
                record.serialize(archive, Records.Headers.TAG);
            }
        }

        public static Class<? extends Operation.Response> typeOf(OpCode opcode) {
            return responseTypes.get(opcode);
        }

        public static Operation.Response deserialize(OpCode op,
                InputArchive archive) throws IOException {
            Operation.Response instance = getInstance().get(op);
            instance.deserialize(archive, tagOf(op));
            return instance;
        }

        public static void serialize(Operation.Response record, OutputArchive archive)
                throws IOException {
            record.serialize(archive, tagOf(record.opcode()));
        }
        
        private Responses() {}
        
        @Override
        public Operation.Response get(OpCode opcode) {
            Class<? extends Operation.Response> type = typeOf(opcode);
            if (type == null) {
                throw new IllegalArgumentException(
                        String.format("No type for %s", opcode));
            } else {
                Operation.Response record = Records.create(type);
                return record;
            }
        }
    }
    
    private Records() {}
}
