package edu.uw.zookeeper.protocol.proto;


import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.BiMap;
import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.ParameterizedFactory;


/**
 * Utility functions for <code>Record</code> objects and types.
 * 
 * @see org.apache.zookeeper.proto
 */
public abstract class Records {
   
    public static interface Header extends Record {}
    
    public static interface Coded extends Record, Operation.Coded {}

    public static interface Request extends Coded, Operation.Request {}

    public static interface Response extends Coded, Operation.Response {}

    public static interface MultiOpRequest extends Request {}
    public static interface MultiOpResponse extends Response {}

    public static interface ConnectGetter {
        int getProtocolVersion();
        int getTimeOut();
        long getSessionId();
        byte[] getPasswd();
    }
    
    public static interface ConnectSetter extends ConnectGetter {
        void setProtocolVersion(int version);
        void setTimeOut(int timeOut);
        void setSessionId(long sessionId);
        void setPasswd(byte[] passwd);
    }
    
    public static interface CreateStatGetter {
        long getCzxid();
        long getCtime();
        long getEphemeralOwner();
    }

    public static interface CreateStatSetter extends CreateStatGetter {
        void setCzxid(long czxid);
        void setCtime(long ctime);
        void setEphemeralOwner(long ephemeralOwner);
    }

    public static interface DataStatGetter {
        long getMzxid();
        long getMtime();
        int getVersion();
    }

    public static interface DataStatSetter extends DataStatGetter {
        void setMzxid(long mzxid);
        void setMtime(long mtime);
        void setVersion(int version);
    }
    
    public static interface AclStatGetter {
        int getAversion();
    }
    
    public static interface AclStatSetter extends AclStatGetter {
        void setAversion(int aversion);
    }

    public static interface ChildrenStatGetter {
        int getCversion();
        long getPzxid();
    }
    
    public static interface ChildrenStatSetter extends ChildrenStatGetter {
        // pzxid seems to be the zxid related to cversion
        void setCversion(int cversion);
        void setPzxid(long pzxid);
    }
    
    public static interface StatPersistedGetter extends CreateStatGetter, DataStatGetter, AclStatGetter, ChildrenStatGetter {
    }

    public static interface StatPersistedSetter extends StatPersistedGetter, CreateStatSetter, DataStatSetter, AclStatSetter, ChildrenStatSetter {
    }
    
    public static interface ZNodeStatGetter extends StatPersistedGetter {
        int getDataLength();
        int getNumChildren();
    }

    public static interface ZNodeStatSetter extends ZNodeStatGetter, StatPersistedSetter {
        void setDataLength(int dataLength);
        void setNumChildren(int numChildren);
    }

    public static interface ZNodeView {}
    
    public static interface PathGetter extends ZNodeView {
        String getPath();
    }
    
    public static interface PathSetter extends PathGetter {
        void setPath(String path);
    }
    
    public static interface StatGetter extends ZNodeView {
        Stat getStat();
    }
    
    public static interface StatSetter extends StatGetter {
        void setStat(Stat stat);
    }
    
    public static interface DataGetter extends ZNodeView {
        byte[] getData();    
    }

    public static interface DataSetter extends DataGetter {
        void setData(byte[] data);     
    }
    
    public static interface AclGetter extends ZNodeView {
        List<ACL> getAcl();     
    }

    public static interface AclSetter extends AclGetter {
        void setAcl(List<ACL> acl);       
    }
    
    public static interface ChildrenGetter extends ZNodeView {
        List<String> getChildren();
    }

    public static interface ChildrenSetter extends ChildrenGetter {
        void setChildren(List<String> children);
    }
    
    public static interface VersionGetter extends ZNodeView {
        int getVersion();
    }
    
    public static interface VersionSetter extends VersionGetter {
        void setVersion(int version);
    }

    public static interface WatchGetter {
        boolean getWatch();
    }

    public static interface WatchSetter extends WatchGetter {
        void setWatch(boolean watch);
    }

    public static interface CreateModeGetter extends PathGetter, DataGetter, AclGetter {
        int getFlags();
    }
    
    public static interface CreateModeSetter extends CreateModeGetter, PathSetter, DataSetter, AclSetter {
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
                return xid.getOpcode();
            } else {
                return null;
            }
        } else {
            return operational.value()[0];
        }
    }
    
    public static OpCodeXid opCodeXidOf(Class<?> type) {
        OperationalXid operationalXid = type.getAnnotation(OperationalXid.class);
        if (operationalXid != null) {
            return operationalXid.value();
        } else {
            return null;
        }
    }

    public static <T extends Record> T newInstance(Class<T> type) {
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
    
    @SuppressWarnings("unchecked")
    public static <T extends Record> T getShared(Class<T> type) {
        T instance = (T) sharedInstances.get(type);
        if (instance == null) {
            if (type.getAnnotation(Shared.class) == null) {
                throw new IllegalArgumentException(type.toString());
            }
            try {
                instance = type.getConstructor().newInstance();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            sharedInstances.putIfAbsent(type, instance);
            instance = (T) sharedInstances.get(type);
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
        TO_BEAN_STRING;
        
        @Override
        public @Nullable String apply(@Nullable Object input) {
            return toBeanString(input);
        }
    }
    
    /**
     * Bean property string.
     */
    public static String toBeanString(Object obj) {
        Objects.ToStringHelper helper = Objects.toStringHelper(obj);
        BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(obj.getClass(), Object.class);
        } catch (IntrospectionException e) {
            throw new AssertionError(e);
        }
        for (PropertyDescriptor pd : beanInfo.getPropertyDescriptors()) {
            Object value;
            try {
                value = pd.getReadMethod().invoke(obj);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            if (value != null) {
                if (value instanceof Record) {
                    value = toBeanString((Record) value);
                } else if (value.getClass().isArray()) {
                    value = String.format("%s{%d}", value.getClass().getComponentType(), Array.getLength(value));
                } else if (value instanceof Iterable) {
                    Iterator<?> itr = ((Iterable<?>) value).iterator();
                    if (itr.hasNext() && (itr.next() instanceof Record)) {
                        itr = ((Iterable<?>) value).iterator();
                        value = iteratorToBeanString(itr);
                    }
                }
            }
            helper.add(pd.getName(), value);
        }
        return helper.toString();
    }
    
    public static String iteratorToBeanString(Iterator<?> value) {
        StringBuilder sb = new StringBuilder().append('[');
        return Joiner.on(',').appendTo(
                sb,
                Iterators.transform(
                        value, 
                        ToBeanString.TO_BEAN_STRING))
                .append(']').toString();
    }
    
    private static final ConcurrentMap<Class<? extends Record>, Record> sharedInstances = 
            new ConcurrentHashMap<Class<? extends Record>, Record>();

    public static abstract class Headers {

        private Headers() {}
        
        // Used for RequestHeader/ReplyHeader
        public static final String TAG = "header";
    }

    public static enum Requests implements ParameterizedFactory<OpCode, Records.Request> {
        INSTANCE;

        public static Requests getInstance() {
            return INSTANCE;
        }

        @SuppressWarnings("unchecked")
        private static final BiMap<OpCode, Class<? extends Records.Request>> requestTypes = 
                Maps.unmodifiableBiMap(EnumHashBiMap.create(
                        Maps.<OpCode, Class<? extends Records.Request>>uniqueIndex(
                                ImmutableList.<Class<? extends Records.Request>>of(
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

        public static Class<? extends Records.Request> typeOf(OpCode opcode) {
            return requestTypes.get(opcode);
        }

        public static Records.Request deserialize(OpCode op,
                InputArchive archive) throws IOException {
            Records.Request instance = getInstance().get(op);
            instance.deserialize(archive, tagOf(op));
            return instance;
        }

        public static void serialize(Records.Request record, OutputArchive archive)
                throws IOException {
            record.serialize(archive, tagOf(record.getOpcode()));
        }
        
        @Override
        public Records.Request get(OpCode opcode) {
            Class<? extends Records.Request> type = typeOf(opcode);
            if (type == null) {
                throw new IllegalArgumentException(
                        String.format("No type for %s", opcode));
            }
            Records.Request record = Records.newInstance(type);
            return record;
        }

        public static abstract class Headers extends Records.Headers {
            
            private Headers() {}
            
            public static Class<IRequestHeader> typeOf() {
                return IRequestHeader.class;
            }
        
            public static IRequestHeader newInstance(int xid, OpCode op) {
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
                serialize(newInstance(xid, op), archive);
            }
        
            public static void serialize(IRequestHeader record, OutputArchive archive)
                    throws IOException {
                record.serialize(archive, Records.Headers.TAG);
            }
        }
    }

    public static enum Responses implements ParameterizedFactory<OpCode, Records.Response> {
        INSTANCE;
        
        public static Responses getInstance() {
            return INSTANCE;
        }

        @SuppressWarnings("unchecked")
        private static final BiMap<OpCode, Class<? extends Records.Response>> responseTypes = 
                Maps.unmodifiableBiMap(EnumHashBiMap.create(
                        Maps.<OpCode, Class<? extends Records.Response>>uniqueIndex(
                                ImmutableList.<Class<? extends Records.Response>>of(
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

        public static Class<? extends Records.Response> typeOf(OpCode opcode) {
            return responseTypes.get(opcode);
        }

        public static Records.Response deserialize(OpCode op,
                InputArchive archive) throws IOException {
            Records.Response instance = getInstance().get(op);
            instance.deserialize(archive, tagOf(op));
            return instance;
        }

        public static void serialize(Records.Response record, OutputArchive archive)
                throws IOException {
            record.serialize(archive, tagOf(record.getOpcode()));
        }
        
        private Responses() {}
        
        @Override
        public Records.Response get(OpCode opcode) {
            Class<? extends Records.Response> type = typeOf(opcode);
            if (type == null) {
                throw new IllegalArgumentException(
                        String.format("No type for %s", opcode));
            } else {
                Records.Response record = Records.newInstance(type);
                return record;
            }
        }

        public static class Headers extends Records.Headers {
            
            private Headers() {}
            
            public static Class<IReplyHeader> typeOf() {
                return IReplyHeader.class;
            }
        
            public static IReplyHeader newInstance(int xid, long zxid,
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
                serialize(newInstance(xid, zxid, code), archive);
            }
        
            public static void serialize(IReplyHeader record, OutputArchive archive)
                    throws IOException {
                record.serialize(archive, Records.Headers.TAG);
            }
        }
    }
    
    private Records() {}
}
