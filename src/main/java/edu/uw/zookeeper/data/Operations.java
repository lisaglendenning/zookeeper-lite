package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.common.Singleton;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.*;

public abstract class Operations {

    public static interface Builder<T extends Records.Coded> {

        OpCode getOpCode();
        
        T build();
    }
    
    public static interface PathBuilder<T extends Records.Coded, C extends PathBuilder<T,C>> extends Builder<T> {
        ZNodeLabel.Path getPath();
        C setPath(ZNodeLabel.Path path);
    }

    public static interface DataBuilder<T extends Records.Coded, C extends DataBuilder<T,C>> extends Builder<T> {
        byte[] getData();
        C setData(byte[] data);
    }

    public static abstract class AbstractBuilder<T extends Records.Coded> implements Builder<T> {
    
        protected OpCode opcode;
        
        protected AbstractBuilder(OpCode opcode) {
            this.opcode = opcode;
        }
        
        @Override
        public OpCode getOpCode() {
            return opcode;
        }
    
        protected Builder<T> setOpCode(OpCode opcode) {
            this.opcode = opcode;
            return this;
        }
    }

    public static abstract class AbstractPath<T extends Records.Coded, C extends AbstractPath<T,C>> extends AbstractBuilder<T> implements PathBuilder<T,C> {
    
        protected ZNodeLabel.Path path;
        
        protected AbstractPath(OpCode opcode) {
            super(opcode);
            this.path = ZNodeLabel.Path.root();
        }
    
        protected AbstractPath(OpCode opcode, ZNodeLabel.Path path) {
            super(opcode);
            this.path = path;
        }
    
        @Override
        public ZNodeLabel.Path getPath() {
            return path;
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public C setPath(ZNodeLabel.Path path) {
            this.path = checkNotNull(path);
            return (C) this;
        }
    }

    public static abstract class Requests {

        public static interface VersionBuilder<T extends Records.Request, C extends VersionBuilder<T,C>> extends Builder<T> {
            int getVersion();
            C setVersion(int version);
        }

        public static interface WatchBuilder<T extends Records.Request, C extends WatchBuilder<T,C>> extends Builder<T> {
            boolean getWatch();
            C setWatch(boolean watch);
        }
        
        public static abstract class AbstractWatch<T extends Records.Request, C extends AbstractWatch<T,C>> extends AbstractPath<T,C> implements WatchBuilder<T, AbstractWatch<T,C>> {
    
            protected boolean watch;
            
            protected AbstractWatch(OpCode opcode) {
                super(opcode);
                this.watch = false;
            }

            protected AbstractWatch(OpCode opcode, ZNodeLabel.Path path, boolean watch) {
                super(opcode, path);
                this.watch = watch;
            }
    
            @Override
            public boolean getWatch() {
                return watch;
            }

            @Override
            @SuppressWarnings("unchecked")
            public C setWatch(boolean watch) {
                this.watch = watch;
                return (C) this;
            }
        }

        public static abstract class AbstractVersion<T extends Records.Request, C extends AbstractVersion<T,C>> extends AbstractPath<T,C> implements VersionBuilder<T, AbstractVersion<T,C>> {
    
            protected int version;
            
            protected AbstractVersion(OpCode opcode) {
                super(opcode);
                this.version = Stats.VERSION_ANY;
            }

            protected AbstractVersion(OpCode opcode, ZNodeLabel.Path path, int version) {
                super(opcode, path);
                this.version = version;
            }

            @Override
            public int getVersion() {
                return version;
            }

            @Override
            @SuppressWarnings("unchecked")
            public C setVersion(int version) {
                this.version = version;
                return (C) this;
            }
        }

        public static abstract class AbstractData<T extends Records.Request, C extends AbstractData<T,C>> extends AbstractPath<T,C> implements DataBuilder<T,C> {
            protected byte[] data;
            
            protected AbstractData(OpCode opcode) {
                super(opcode);
                this.data = new byte[0];
            }

            protected AbstractData(OpCode opcode, ZNodeLabel.Path path, byte[] data) {
                super(opcode, path);
                this.data = data;
            }
            
            @Override
            public byte[] getData() {
                return data;
            }
            
            @Override
            @SuppressWarnings("unchecked")
            public C setData(byte[] data) {
                this.data = checkNotNull(data);
                return (C) this;
            }
        }

        public static class Check extends AbstractVersion<ICheckVersionRequest, Check> {

            public static Check fromRecord(ICheckVersionRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                int version = request.getVersion();
                return new Check(path, version);
            }
            
            public Check() {
                super(OpCode.CHECK);
            }

            public Check(ZNodeLabel.Path path, int version) {
                super(OpCode.DELETE, path, version);
            }

            @Override
            public ICheckVersionRequest build() {
                ICheckVersionRequest record = new ICheckVersionRequest(
                        getPath().toString(), getVersion());
                return record;
            }
        }
        
        public static class Create extends AbstractData<Records.Request, Create> {
            
            public static Create fromRecord(Records.Request request) {
                Records.CreateModeGetter record = (Records.CreateModeGetter) request;
                OpCode opcode = request.opcode();
                ZNodeLabel.Path path = ZNodeLabel.Path.of(record.getPath());
                byte[] data = record.getData();
                CreateMode mode = CreateMode.valueOf(record.getFlags());
                List<Acls.Acl> acl = Acls.Acl.fromRecordList(record.getAcl());
                return new Create(opcode, path, data, mode, acl);
            }
            
            protected CreateMode mode;
            protected List<Acls.Acl> acl;
            
            public Create() {
                super(OpCode.CREATE);
                this.mode = CreateMode.PERSISTENT;
                this.acl = Acls.Definition.ANYONE_ALL.asList();
            }

            public Create(OpCode opcode, ZNodeLabel.Path path, byte[] data, CreateMode mode, List<Acls.Acl> acl) {
                super(opcode, path, data);
                this.mode = mode;
                this.acl = acl;
            }
            
            public CreateMode getMode() {
                return mode;
            }
            
            public Create setMode(CreateMode mode) {
                this.mode = checkNotNull(mode);
                return this;
            }
            
            public boolean getStat() {
                return (getOpCode() == OpCode.CREATE2);
            }
            
            /**
             * >= 3.5.0
             */
            public Create setStat(boolean getStat) {
                setOpCode(getStat? OpCode.CREATE2 : OpCode.CREATE);
                return this;
            }
            
            public List<Acls.Acl> getAcl() {
                return acl;
            }
            
            public Create setAcl(List<Acls.Acl> acl) {
                this.acl = checkNotNull(acl);
                return this;
            }
            
            @Override
            public Records.Request build() {
                String path = getPath().toString();
                byte[] data = getData();
                List<ACL> acls = Acls.Acl.asRecordList(getAcl());
                int mode = getMode().intValue();
                Records.Request record = getStat() 
                        ? new ICreate2Request(path, data, acls, mode) 
                        : new ICreateRequest(path, data, acls, mode);
                return record;
            }
        }
        
        public static class Delete extends AbstractVersion<IDeleteRequest, Delete> {

            public static Delete fromRecord(IDeleteRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                int version = request.getVersion();
                return new Delete(path, version);
            }

            public Delete() {
                super(OpCode.DELETE);
            }

            public Delete(ZNodeLabel.Path path, int version) {
                super(OpCode.DELETE, path, version);
            }
            
            @Override
            public IDeleteRequest build() {
                IDeleteRequest record = new IDeleteRequest(
                        getPath().toString(), getVersion());
                return record;
            }
        }

        public static class Exists extends AbstractWatch<IExistsRequest, Exists> {

            public static Exists fromRecord(IExistsRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                boolean watch = request.getWatch();
                return new Exists(path, watch);
            }
            
            public Exists() {
                super(OpCode.EXISTS);
            }
            
            public Exists(ZNodeLabel.Path path, boolean watch) {
                super(OpCode.EXISTS, path, watch);
            }

            @Override
            public IExistsRequest build() {
                IExistsRequest record = new IExistsRequest(
                        getPath().toString(), getWatch());
                return record;
            }
        }
        
        public static class GetAcl extends AbstractPath<IGetACLRequest, GetAcl> {

            public static GetAcl fromRecord(IGetACLRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                return new GetAcl(path);
            }
            
            public GetAcl() {
                super(OpCode.GET_ACL);
            }

            public GetAcl(ZNodeLabel.Path path) {
                super(OpCode.GET_ACL, path);
            }

            @Override
            public IGetACLRequest build() {
                return new IGetACLRequest(getPath().toString());
            }
        }
        
        public static class GetChildren extends AbstractWatch<Records.Request, GetChildren> {

            public static GetChildren fromRecord(Records.Request request) {
                OpCode opcode = request.opcode();
                ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter)request).getPath());
                boolean watch = ((Records.WatchGetter)request).getWatch();
                return new GetChildren(opcode, path, watch);
            }
            
            public GetChildren() {
                super(OpCode.GET_CHILDREN);
            }

            public GetChildren(OpCode opcode, ZNodeLabel.Path path, boolean watch) {
                super(opcode, path, watch);
            }
            
            public boolean getStat() {
                return (getOpCode() == OpCode.GET_CHILDREN2);
            }
            
            public GetChildren setStat(boolean getStat) {
                setOpCode(getStat ? OpCode.GET_CHILDREN2 : OpCode.GET_CHILDREN);
                return this;
            }
            
            @Override
            public Records.Request build() {
                Records.Request record = getStat()
                        ? new IGetChildren2Request(getPath().toString(), getWatch())
                        : new IGetChildrenRequest(getPath().toString(), getWatch());
                return record;
            }
        }

        public static class GetData extends AbstractWatch<IGetDataRequest, GetData> {

            public static GetData fromRecord(IGetDataRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                boolean watch = request.getWatch();
                return new GetData(path, watch);
            }
            
            public GetData() {
                super(OpCode.GET_DATA);
            }

            public GetData(ZNodeLabel.Path path, boolean watch) {
                super(OpCode.GET_DATA, path, watch);
            }

            @Override
            public IGetDataRequest build() {
                IGetDataRequest record = new IGetDataRequest(
                        getPath().toString(), getWatch());
                return record;
            }
        }
        
        public static class Multi extends AbstractBuilder<IMultiRequest> implements Iterable<AbstractBuilder<? extends Records.Request>> {

            public static Multi fromRecord(IMultiRequest request) {
                Multi builder = new Multi();
                for (Records.MultiOpRequest e: request){
                    builder.add(Requests.fromRecord(e));
                }
                return builder;
            }
            
            protected List<AbstractBuilder<? extends Records.Request>> builders;
            
            public Multi() {
                super(OpCode.MULTI);
                this.builders = Lists.newArrayList();
            }
            
            public Multi add(AbstractBuilder<? extends Records.Request> builder) {
                builders.add(builder);
                return this;
            }

            @Override
            public Iterator<AbstractBuilder<? extends Records.Request>> iterator() {
                return builders.iterator();
            }
            
            @Override
            public IMultiRequest build() {
                ImmutableList.Builder<Records.MultiOpRequest> ops = ImmutableList.builder();
                for (Builder<? extends Records.Request> builder: this) {
                    ops.add((Records.MultiOpRequest) builder.build());
                }
                return new IMultiRequest(ops.build());
            }
        }

        public static class SetAcl extends AbstractVersion<ISetACLRequest, SetAcl> {

            public static SetAcl fromRecord(ISetACLRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                int version = request.getVersion();
                List<Acls.Acl> acl = Acls.Acl.fromRecordList(request.getAcl());
                return new SetAcl(path, version, acl);
            }
            
            protected List<Acls.Acl> acl;
            
            public SetAcl() {
                super(OpCode.SET_ACL);
                this.acl = Acls.Definition.ANYONE_ALL.asList();
            }

            public SetAcl(ZNodeLabel.Path path, int version, List<Acls.Acl> acl) {
                super(OpCode.SET_ACL, path, version);
                this.acl = acl;
            }
            
            public List<Acls.Acl> getAcl() {
                return acl;
            }
            
            public SetAcl setAcl(List<Acls.Acl> acls) {
                this.acl = checkNotNull(acls);
                return this;
            }
            
            @Override
            public ISetACLRequest build() {
                ISetACLRequest record = new ISetACLRequest(
                        getPath().toString(), Acls.Acl.asRecordList(getAcl()), getVersion());
                return record;
            }
        }

        public static class SetData extends AbstractData<ISetDataRequest, SetData> implements VersionBuilder<ISetDataRequest, SetData> {

            public static SetData fromRecord(ISetDataRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                byte[] data = request.getData();
                int version = request.getVersion();
                return new SetData(path, data, version);
            }
            
            protected int version;
            
            public SetData() {
                super(OpCode.SET_DATA);
                this.version = -1;
            }

            public SetData(ZNodeLabel.Path path, byte[] data, int version) {
                super(OpCode.SET_DATA, path, data);
                this.version = version;
            }

            public int getVersion() {
                return version;
            }
            
            public SetData setVersion(int version) {
                this.version = version;
                return this;
            }
            
            @Override
            public ISetDataRequest build() {
                ISetDataRequest record = new ISetDataRequest(
                        getPath().toString(), getData(), getVersion());
                return record;
            }
        }
        
        public static class Sync extends AbstractPath<ISyncRequest, Sync> {

            public static Sync fromRecord(ISyncRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                return new Sync(path);
            }
            
            public Sync() {
                super(OpCode.SYNC);
            }

            public Sync(ZNodeLabel.Path path) {
                super(OpCode.SYNC, path);
            }

            @Override
            public ISyncRequest build() {
                return new ISyncRequest(getPath().toString());
            }
        }
        
        public static class SerializedData<T extends Records.Request, C extends AbstractData<T,C>, V> extends AbstractBuilder<T> implements PathBuilder<T,SerializedData<T,C,V>>, DataBuilder<T,SerializedData<T,C,V>> {

            public static <T extends Records.Request, C extends AbstractData<T,C>, V> SerializedData<T,C,V> of (C delegate, Serializers.ByteSerializer<? super V> serializer, V input) {
                return new SerializedData<T,C,V>(delegate, serializer, input);
            }
            
            protected final C delegate;
            protected final Serializers.ByteSerializer<? super V> serializer;
            protected final V input;
            
            public SerializedData(C delegate, Serializers.ByteSerializer<? super V> serializer, V input) {
                super(delegate.getOpCode());
                this.delegate = delegate;
                this.input = input;
                this.serializer = serializer;
            }
            
            public V input() {
                return input;
            }
            
            public Serializers.ByteSerializer<? super V> serializer() {
                return serializer;
            }
            
            public C delegate() {
                return delegate;
            }

            @Override
            public T build() {
                if (input() != null) {
                    try {
                        delegate().setData(serializer().toBytes(input()));
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
                return delegate().build();
            }

            @Override
            public ZNodeLabel.Path getPath() {
                return delegate().getPath();
            }

            @Override
            public SerializedData<T,C,V> setPath(edu.uw.zookeeper.data.ZNodeLabel.Path path) {
                delegate().setPath(path);
                return this;
            }

            @Override
            public byte[] getData() {
                return delegate().getData();
            }

            @Override
            public SerializedData<T,C,V> setData(byte[] data) {
                delegate.setData(data);
                return this;
            }
        }
        
        public static AbstractBuilder<? extends Records.Request> fromRecord(Records.Request record) {
            switch (record.opcode()) {
            case CHECK:
                return Check.fromRecord((ICheckVersionRequest) record);
            case CREATE:
            case CREATE2:
                return Create.fromRecord(record);
            case DELETE:
                return Delete.fromRecord((IDeleteRequest) record);
            case EXISTS:
                return Exists.fromRecord((IExistsRequest) record);
            case GET_ACL:
                return GetAcl.fromRecord((IGetACLRequest) record);
            case GET_CHILDREN:
            case GET_CHILDREN2:
                return GetChildren.fromRecord(record);
            case GET_DATA:
                return GetData.fromRecord((IGetDataRequest) record);
            case MULTI:
                return Multi.fromRecord((IMultiRequest) record);
            case SET_ACL:
                return SetAcl.fromRecord((ISetACLRequest) record);
            case SET_DATA:
                return SetData.fromRecord((ISetDataRequest) record);
            case SYNC:
                return Sync.fromRecord((ISyncRequest) record);
            default:
                throw new IllegalArgumentException(record.toString());
            }
        }

        public static AbstractBuilder<? extends Records.Request> fromOpCode(OpCode opcode) {
            switch (opcode) {
            case CHECK:
                return check();
            case CREATE:
                return create();
            case CREATE2:
                return create().setStat(true);
            case DELETE:
                return delete();
            case EXISTS:
                return exists();
            case GET_ACL:
                return getAcl();
            case GET_CHILDREN:
                return getChildren();
            case GET_CHILDREN2:
                return getChildren().setStat(true);
            case GET_DATA:
                return getData();
            case MULTI:
                return multi();
            case SET_ACL:
                return setAcl();
            case SET_DATA:
                return setData();
            case SYNC:
                return sync();
            default:
                throw new IllegalArgumentException(opcode.toString());
            }
        }
        
        public static Check check() {
            return new Check();
        }
        
        public static Create create() {
            return new Create();
        }
        
        public static Delete delete() {
            return new Delete();
        }

        public static Exists exists() {
            return new Exists();
        }

        public static GetAcl getAcl() {
            return new GetAcl();
        }
        
        public static GetChildren getChildren() {
            return new GetChildren();
        }

        public static GetData getData() {
            return new GetData();
        }
        
        public static Multi multi() {
            return new Multi();
        }

        public static SetAcl setAcl() {
            return new SetAcl();
        }

        public static SetData setData() {
            return new SetData();
        }
        
        public static Sync sync() {
            return new Sync();
        }
        
        public static <T extends Records.Request, C extends AbstractData<T,C>, V> SerializedData<T,C,V> serialized(C delegate, Serializers.ByteSerializer<? super V> serializer, V input) {
            return SerializedData.of(delegate, serializer, input);
        }
        
        private Requests() {}
    }
    
    public static abstract class Responses {

        public static interface StatBuilder<T extends Records.Response, C extends StatBuilder<T,C>> extends Builder<T> {
            Stat getStat();
            C setStat(Stat stat);
        }
        
        public static abstract class AbstractStat<T extends Records.Response, C extends AbstractStat<T,C>> extends AbstractBuilder<T> implements StatBuilder<T,C> {
    
            protected Stat stat;
            
            protected AbstractStat(OpCode opcode) {
                super(opcode);
                this.stat = null;
            }
            
            protected AbstractStat(OpCode opcode, Stat stat) {
                super(opcode);
                this.stat = stat;
            }

            @Override
            public Stat getStat() {
                return stat;
            }

            @Override
            @SuppressWarnings("unchecked")
            public C setStat(Stat stat) {
                this.stat = stat;
                return (C) this;
            }
        }

        public static class Check extends AbstractStat<ICheckVersionResponse, Check> {

            public static Check fromRecord(ICheckVersionResponse record) {
                Stat stat = record.getStat();
                return new Check(stat);
            }
            
            public Check() {
                super(OpCode.CHECK);
            }
            
            public Check(Stat stat) {
                super(OpCode.CHECK, stat);
            }
            
            @Override
            public ICheckVersionResponse build() {
                return new ICheckVersionResponse(getStat());
            }
        }
    
        public static class Create extends AbstractStat<Records.Response, Create> implements PathBuilder<Records.Response, Create> {

            public static Create fromRecord(Records.Response record) {
                OpCode opcode = record.opcode();
                ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) record).getPath());
                Stat stat = null;
                if (record instanceof Records.StatGetter) {
                    stat = ((Records.StatGetter) record).getStat();
                }
                return new Create(opcode, stat, path);
            }
            
            protected ZNodeLabel.Path path;
            
            public Create() {
                super(OpCode.CREATE);
                this.path = ZNodeLabel.Path.root();
            }
            
            public Create(OpCode opcode, Stat stat, ZNodeLabel.Path path) {
                super(opcode, stat);
                this.path = path;
            }
            
            @Override
            public ZNodeLabel.Path getPath() {
                return path;
            }

            @Override
            public Create setPath(ZNodeLabel.Path path) {
                this.path = path;
                return this;
            }
    
            @Override
            public Create setStat(Stat stat) {
                setOpCode((stat != null) ? OpCode.CREATE2 : OpCode.CREATE);
                super.setStat(stat);
                return this;
            }
            
            @Override
            public Records.Response build() {
                Records.Response record = (getOpCode() == OpCode.CREATE2)
                        ? new ICreate2Response(getPath().toString(), getStat())
                        : new ICreateResponse(getPath().toString());
                return record;
            }
        }
        
        public static class Delete extends AbstractBuilder<IDeleteResponse> {

            public static Delete getInstance() {
                return Holder.INSTANCE.get();
            }
            
            public static enum Holder implements Singleton<Delete> {
                INSTANCE;

                private final Delete instance = new Delete();
                
                @Override
                public Delete get() {
                    return instance;
                }
            }
            
            public Delete() {
                super(OpCode.DELETE);
            }

            @Override
            public IDeleteResponse build() {
                return Records.newInstance(IDeleteResponse.class);
            }
        }
    
        public static class Exists extends AbstractStat<IExistsResponse, Exists> {

            public static Exists fromRecord(IExistsResponse record) {
                Stat stat = record.getStat();
                return new Exists(stat);
            }
            
            public Exists() {
                super(OpCode.EXISTS);
            }

            public Exists(Stat stat) {
                super(OpCode.EXISTS, stat);
            }
            
            @Override
            public IExistsResponse build() {
                return new IExistsResponse(getStat());
            }
        }
        
        public static class Error extends AbstractBuilder<IErrorResponse> {

            public static Error fromRecord(IErrorResponse record) {
                return new Error(record.error());
            }
            
            protected KeeperException.Code error;
            
            public Error() {
                this(KeeperException.Code.OK);
            }

            public Error(KeeperException.Code error) {
                super(OpCode.ERROR);
                this.error = error;
            }
            
            public KeeperException.Code getError() {
                return error;
            }

            public Error setError(KeeperException.Code error) {
                this.error = error;
                return this;
            }

            @Override
            public IErrorResponse build() {
                return new IErrorResponse(getError());
            }
        }
    
        public static class GetAcl extends AbstractStat<IGetACLResponse, GetAcl> {

            public static GetAcl fromRecord(IGetACLResponse record) {
                Stat stat = record.getStat();
                List<Acls.Acl> acl = Acls.Acl.fromRecordList(record.getAcl());
                return new GetAcl(stat, acl);
            }
            
            protected List<Acls.Acl> acl;
            
            public GetAcl() {
                super(OpCode.GET_ACL);
                this.acl = ImmutableList.of();
            }
            
            public GetAcl(Stat stat, List<Acls.Acl> acl) {
                super(OpCode.GET_ACL, stat);
                this.acl = acl;
            }
    
            public List<Acls.Acl> getAcl() {
                return acl;
            }
            
            public GetAcl setAcl(List<Acls.Acl> acl) {
                this.acl = acl;
                return this;
            }
            
            @Override
            public IGetACLResponse build() {
                return new IGetACLResponse(Acls.Acl.asRecordList(getAcl()), getStat());
            }
        }
        
        public static class GetChildren extends AbstractStat<Records.Response, GetChildren> {

            public static GetChildren fromRecord(Records.Response record) {
                OpCode opcode = record.opcode();
                Stat stat = null;
                if (record instanceof Records.StatGetter) {
                    stat = ((Records.StatGetter) record).getStat();
                }
                List<String> childrenStr = ((Records.ChildrenGetter) record).getChildren();
                List<ZNodeLabel.Component> children = Lists.newArrayListWithCapacity(childrenStr.size());
                for (String e: childrenStr) {
                    children.add(ZNodeLabel.Component.of(e));
                }
                return new GetChildren(opcode, stat, children);
            }
            
            protected Iterable<ZNodeLabel.Component> children;
            
            public GetChildren() {
                super(OpCode.GET_CHILDREN);
                this.children = ImmutableList.of();
            }

            public GetChildren(OpCode opcode, Stat stat, Iterable<ZNodeLabel.Component> children) {
                super(opcode, stat);
                this.children = children;
            }
    
            @Override
            public GetChildren setStat(Stat stat) {
                setOpCode((stat != null) ? OpCode.GET_CHILDREN2 : OpCode.GET_CHILDREN);
                super.setStat(stat);
                return this;
            }
            
            public Iterable<ZNodeLabel.Component> getChildren() {
                return children;
            }
            
            public GetChildren setChildren(Iterable<ZNodeLabel.Component> children) {
                this.children = children;
                return this;
            }
            
            @Override
            public Records.Response build() {
                List<String> children = ImmutableList.copyOf(Iterables.transform(getChildren(), Functions.toStringFunction()));
                Records.Response record = (OpCode.GET_CHILDREN2 == getOpCode())
                        ? new IGetChildren2Response(children, getStat())
                        : new IGetChildrenResponse(children);
                return record;
            }
        }
        
        public static class GetData extends AbstractStat<IGetDataResponse, GetData> implements DataBuilder<IGetDataResponse, GetData> {

            public static GetData fromRecord(IGetDataResponse record) {
                Stat stat = record.getStat();
                byte[] data = record.getData();
                return new GetData(stat, data);
            }
            
            protected byte[] data;
            
            public GetData() {
                super(OpCode.GET_DATA, Stats.ImmutableStat.uninitialized());
                this.data = new byte[0];
            }

            public GetData(Stat stat, byte[] data) {
                super(OpCode.GET_DATA, stat);
                this.data = data;
            }
            
            @Override
            public byte[] getData() {
                return data;
            }
    
            @Override
            public GetData setData(byte[] data) {
                this.data = data;
                return this;
            }

            @Override
            public IGetDataResponse build() {
                return new IGetDataResponse(getData(), getStat());
            }
        }
    
        public static class Multi extends AbstractBuilder<IMultiResponse> implements Iterable<Builder<? extends Records.Response>> {

            public static Multi fromRecord(IMultiResponse record) {
                Multi builder = new Multi();
                for (Records.MultiOpResponse e: record){
                    builder.add(Responses.fromRecord(e));
                }
                return builder;
            }
            
            protected List<Builder<? extends Records.Response>> builders;
            
            public Multi() {
                super(OpCode.MULTI);
                this.builders = Lists.newArrayList();
            }
            
            public Multi add(Builder<? extends Records.Response> builder) {
                builders.add(builder);
                return this;
            }
    
            @Override
            public Iterator<Builder<? extends Records.Response>> iterator() {
                return builders.iterator();
            }
            
            @Override
            public IMultiResponse build() {
                ImmutableList.Builder<Records.MultiOpResponse> ops = ImmutableList.builder();
                for (Builder<? extends Records.Response> builder: this) {
                    ops.add((Records.MultiOpResponse) builder.build());
                }
                return new IMultiResponse(ops.build());
            }
        }
        
        public static class Notification extends AbstractPath<IWatcherEvent, Notification> {

            public static Notification fromRecord(IWatcherEvent record) {
                return new Notification(
                        ZNodeLabel.Path.of(record.getPath()),
                        EventType.fromInt(record.getType()),
                        KeeperState.fromInt(record.getState()));
            }
            
            private KeeperState keeperState;
            private EventType eventType;
            
            public Notification() {
                super(OpCode.NOTIFICATION);
                this.eventType = EventType.None;
                this.keeperState = KeeperState.SyncConnected;
            }

            public Notification(
                    ZNodeLabel.Path path,
                    EventType eventType,
                    KeeperState keeperState) {
                super(OpCode.NOTIFICATION, path);
                this.eventType = eventType;
                this.keeperState = keeperState;
            }
            
            public KeeperState getKeeperState() {
                return keeperState;
            }
            
            public Notification setKeeperState(KeeperState keeperState) {
                this.keeperState = keeperState;
                return this;
            }
            
            public EventType getEventType() {
                return eventType;
            }

            public Notification setEventType(EventType eventType) {
                this.eventType = eventType;
                return this;
            }
            
            @Override
            public IWatcherEvent build() {
                return new IWatcherEvent( 
                        getEventType().getIntValue(),
                        getKeeperState().getIntValue(),
                        getPath().toString());
            }
        }
    
        public static class SetAcl extends AbstractStat<ISetACLResponse, SetAcl> {

            public static SetAcl fromRecord(ISetACLResponse record) {
                Stat stat = record.getStat();
                return new SetAcl(stat);
            }
            
            public SetAcl() {
                super(OpCode.SET_ACL);
            }

            public SetAcl(Stat stat) {
                super(OpCode.SET_ACL, stat);
            }
            
            @Override
            public ISetACLResponse build() {
                return new ISetACLResponse(getStat());
            }
        }
    
        public static class SetData extends AbstractStat<ISetDataResponse, SetData> {

            public static SetData fromRecord(ISetDataResponse record) {
                Stat stat = record.getStat();
                return new SetData(stat);
            }
            
            public SetData() {
                super(OpCode.SET_DATA);
            }
            
            public SetData(Stat stat) {
                super(OpCode.SET_DATA, stat);
            }
            
            @Override
            public ISetDataResponse build() {
                return new ISetDataResponse(getStat());
            }
        }
    
        public static class Sync extends AbstractPath<ISyncResponse, Sync> {

            public static Sync fromRecord(ISyncResponse record) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(record.getPath());
                return new Sync(path);
            }
            
            public Sync() {
                super(OpCode.SYNC);
            }

            public Sync(ZNodeLabel.Path path) {
                super(OpCode.SYNC, path);
            }

            @Override
            public ISyncResponse build() {
                return new ISyncResponse(getPath().toString());
            }
        }

        public static AbstractBuilder<? extends Records.Response> fromRecord(Records.Response record) {
            switch (record.opcode()) {
            case CHECK:
                return Check.fromRecord((ICheckVersionResponse) record);
            case CREATE:
            case CREATE2:
                return Create.fromRecord(record);
            case DELETE:
                return Delete.getInstance();
            case ERROR:
                return Error.fromRecord((IErrorResponse) record);
            case EXISTS:
                return Exists.fromRecord((IExistsResponse) record);
            case GET_ACL:
                return GetAcl.fromRecord((IGetACLResponse) record);
            case GET_CHILDREN:
            case GET_CHILDREN2:
                return GetChildren.fromRecord(record);
            case GET_DATA:
                return GetData.fromRecord((IGetDataResponse) record);
            case MULTI:
                return Multi.fromRecord((IMultiResponse) record);
            case NOTIFICATION:
                return Notification.fromRecord((IWatcherEvent) record);
            case SET_ACL:
                return SetAcl.fromRecord((ISetACLResponse) record);
            case SET_DATA:
                return SetData.fromRecord((ISetDataResponse) record);
            case SYNC:
                return Sync.fromRecord((ISyncResponse) record);
            default:
                throw new IllegalArgumentException(record.toString());
            }
        }
        
        public static AbstractBuilder<? extends Records.Response> fromOpCode(OpCode opcode) {
            switch (opcode) {
            case CHECK:
                return check();
            case CREATE:
                return create();
            case CREATE2:
                return create().setStat(Stats.ImmutableStat.uninitialized());
            case DELETE:
                return delete();
            case ERROR:
                return error();
            case EXISTS:
                return exists();
            case GET_ACL:
                return getAcl();
            case GET_CHILDREN:
                return getChildren();
            case GET_CHILDREN2:
                return getChildren().setStat(Stats.ImmutableStat.uninitialized());
            case GET_DATA:
                return getData();
            case MULTI:
                return multi();
            case NOTIFICATION:
                return notification();
            case SET_ACL:
                return setAcl();
            case SET_DATA:
                return setData();
            case SYNC:
                return sync();
            default:
                throw new IllegalArgumentException(opcode.toString());
            }
        }

        public static Check check() {
            return new Check();
        }
        
        public static Create create() {
            return new Create();
        }
        
        public static Delete delete() {
            return Delete.getInstance();
        }
    
        public static Error error() {
            return new Error();
        }
        
        public static Exists exists() {
            return new Exists();
        }
    
        public static GetAcl getAcl() {
            return new GetAcl();
        }
    
        public static GetChildren getChildren() {
            return new GetChildren();
        }
    
        public static GetData getData() {
            return new GetData();
        }
        
        public static Multi multi() {
            return new Multi();
        }
        
        public static Notification notification() {
            return new Notification();
        }
        
        public static SetAcl setAcl() {
            return new SetAcl();
        }
        
        public static SetData setData() {
            return new SetData();
        }
        
        public static Sync sync() {
            return new Sync();
        }
    }
    
    public static Builder<? extends Records.Coded> fromRecord(Records.Coded input) {
        if (input instanceof Records.Request) {
            return Requests.fromRecord((Records.Request) input);
        } else {
            return Responses.fromRecord((Records.Response) input);
        }
    }

    public static Records.Response unlessError(Records.Response response) throws KeeperException {
        return Operations.unlessError(response, "Unexpected Error");
    }

    public static Records.Response unlessError(Records.Response response, String message) throws KeeperException {
        if (response instanceof Operation.Error) {
            KeeperException.Code error = ((Operation.Error) response).error();
            throw KeeperException.create(error, message);
        } else {
            return response;
        }
    }

    public static Operation.Error expectError(Records.Response response, KeeperException.Code expected) throws KeeperException {
        return Operations.expectError(response, expected, "Expected Error " + expected.toString());
    }

    public static Operation.Error expectError(Records.Response response, KeeperException.Code expected, String message) throws KeeperException {
        if (response instanceof Operation.Error) {
            Operation.Error error = (Operation.Error) response;
            if (expected != error.error()) {
                throw KeeperException.create(error.error(), message);
            }
            return error;
        } else {
            throw new IllegalArgumentException("Unexpected Response " + response.toString());
        }
    }

    public static Optional<Operation.Error> maybeError(Records.Response response, KeeperException.Code expected) throws KeeperException {
        return Operations.maybeError(response, expected, "Expected Error " + expected.toString());
    }

    public static Optional<Operation.Error> maybeError(Records.Response response, KeeperException.Code expected, String message) throws KeeperException {
        if (response instanceof Operation.Error) {
            Operation.Error error = (Operation.Error) response;
            if (expected != error.error()) {
                throw KeeperException.create(error.error(), message);
            }
            return Optional.of(error);
        }
        return Optional.absent();
    }

    private Operations() {}
}
