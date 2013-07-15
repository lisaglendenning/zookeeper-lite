package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.*;
import edu.uw.zookeeper.util.Singleton;

public abstract class Operations {

    public static interface Builder<T extends Records.Coded> {

        OpCode getOpCode();
        
        T build();
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
    
    public static interface PathBuilder<T extends Records.Coded> extends Builder<T> {
        ZNodeLabel.Path getPath();
        PathBuilder<T> setPath(ZNodeLabel.Path path);
    }

    public static interface DataBuilder<T extends Records.Coded> extends Builder<T> {
        byte[] getData();
        DataBuilder<T> setData(byte[] data);
    }
    
    public static abstract class Requests {
        
        public static abstract class AbstractPath<T extends Records.Request, C extends AbstractPath<T,C>> extends AbstractBuilder<T> implements PathBuilder<T> {
    
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

        public static abstract class AbstractData<T extends Records.Request, C extends AbstractData<T,C>> extends AbstractPath<T,C> implements DataBuilder<T> {
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

        public static class Check extends AbstractPath<ICheckVersionRequest, Check> {

            public static Check fromRecord(ICheckVersionRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                int version = request.getVersion();
                return new Check(path, version);
            }
            
            protected int version;
            
            public Check() {
                super(OpCode.CHECK);
                this.version = -1;
            }

            public Check(ZNodeLabel.Path path, int version) {
                super(OpCode.DELETE, path);
                this.version = version;
            }
            
            public int getVersion() {
                return version;
            }
            
            public Check setVersion(int version) {
                this.version = version;
                return this;
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
                OpCode opcode = request.getOpcode();
                ZNodeLabel.Path path = ZNodeLabel.Path.of(record.getPath());
                byte[] data = record.getData();
                CreateMode mode;
                try {
                    mode = CreateMode.fromFlag(record.getFlags());
                } catch (KeeperException e) {
                    throw new IllegalArgumentException(e);
                }
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
                super(opcode);
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
                Records.Request record = getStat() 
                        ? new ICreate2Request(getPath().toString(), getData(), Acls.Acl.asRecordList(getAcl()), getMode().toFlag()) 
                        : new ICreateRequest(getPath().toString(), getData(), Acls.Acl.asRecordList(getAcl()), getMode().toFlag());
                return record;
            }
        }
        
        public static class Delete extends AbstractPath<IDeleteRequest, Delete> {

            public static Delete fromRecord(IDeleteRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                int version = request.getVersion();
                return new Delete(path, version);
            }
            
            protected int version;
            
            public Delete() {
                super(OpCode.DELETE);
                this.version = -1;
            }

            public Delete(ZNodeLabel.Path path, int version) {
                super(OpCode.DELETE, path);
                this.version = version;
            }
            
            public int getVersion() {
                return version;
            }
            
            public Delete setVersion(int version) {
                this.version = version;
                return this;
            }
            
            @Override
            public IDeleteRequest build() {
                IDeleteRequest record = new IDeleteRequest(
                        getPath().toString(), getVersion());
                return record;
            }
        }

        public static class Exists extends AbstractPath<IExistsRequest, Exists> {

            public static Exists fromRecord(IExistsRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                boolean watch = request.getWatch();
                return new Exists(path, watch);
            }
            
            protected boolean watch;
            
            public Exists() {
                super(OpCode.EXISTS);
                this.watch = false;
            }
            
            public Exists(ZNodeLabel.Path path, boolean watch) {
                super(OpCode.EXISTS, path);
                this.watch = watch;
            }
            
            public boolean getWatch() {
                return watch;
            }
            
            public Exists setWatch(boolean watch) {
                this.watch = watch;
                return this;
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
        
        public static class GetChildren extends AbstractPath<Records.Request, GetChildren> {

            public static GetChildren fromRecord(Records.Request request) {
                OpCode opcode = request.getOpcode();
                ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter)request).getPath());
                boolean watch = ((Records.WatchGetter)request).getWatch();
                return new GetChildren(opcode, path, watch);
            }
            
            protected boolean watch;
            
            public GetChildren() {
                super(OpCode.GET_CHILDREN);
                this.watch = false;
            }

            public GetChildren(OpCode opcode, ZNodeLabel.Path path, boolean watch) {
                super(opcode, path);
                this.watch = watch;
            }
            
            public boolean getWatch() {
                return watch;
            }
            
            public GetChildren setWatch(boolean watch) {
                this.watch = watch;
                return this;
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

        public static class GetData extends AbstractPath<IGetDataRequest, GetData> {

            public static GetData fromRecord(IGetDataRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                boolean watch = request.getWatch();
                return new GetData(path, watch);
            }
            
            protected boolean watch;
            
            public GetData() {
                super(OpCode.GET_DATA);
                this.watch = false;
            }

            public GetData(ZNodeLabel.Path path, boolean watch) {
                super(OpCode.GET_DATA, path);
                this.watch = watch;
            }
            
            public boolean getWatch() {
                return watch;
            }
            
            public GetData setWatch(boolean watch) {
                this.watch = watch;
                return this;
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
                IMultiRequest record = new IMultiRequest();
                for (AbstractBuilder<? extends Records.Request> e: builders) {
                    record.add((Records.MultiOpRequest) e.build());
                }
                return record;
            }
        }

        public static class SetAcl extends AbstractPath<ISetACLRequest, SetAcl> {

            public static SetAcl fromRecord(ISetACLRequest request) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(request.getPath());
                int version = request.getVersion();
                List<Acls.Acl> acl = Acls.Acl.fromRecordList(request.getAcl());
                return new SetAcl(path, version, acl);
            }
            
            protected int version;
            protected List<Acls.Acl> acl;
            
            public SetAcl() {
                super(OpCode.SET_ACL);
                this.version = -1;
                this.acl = Acls.Definition.ANYONE_ALL.asList();
            }

            public SetAcl(ZNodeLabel.Path path, int version, List<Acls.Acl> acl) {
                super(OpCode.SET_ACL, path);
                this.version = version;
                this.acl = acl;
            }
            
            public int getVersion() {
                return version;
            }
            
            public SetAcl setVersion(int version) {
                this.version = version;
                return this;
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

        public static class SetData extends AbstractData<ISetDataRequest, SetData> {

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
        
        public static class SerializedData<T extends Records.Request, C extends AbstractData<T,C>, V> extends AbstractBuilder<T> implements PathBuilder<T>, DataBuilder<T> {

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
            AbstractBuilder<? extends Records.Request> builder = null;
            switch (record.getOpcode()) {
            case CHECK:
                builder = Check.fromRecord((ICheckVersionRequest) record);
                break;
            case CREATE:
            case CREATE2:
                builder = Create.fromRecord(record);
                break;
            case DELETE:
                builder = Delete.fromRecord((IDeleteRequest) record);
                break;
            case EXISTS:
                builder = Exists.fromRecord((IExistsRequest) record);
                break;
            case GET_ACL:
                builder = GetAcl.fromRecord((IGetACLRequest) record);
                break;
            case GET_CHILDREN:
            case GET_CHILDREN2:
                builder = GetChildren.fromRecord(record);
                break;
            case GET_DATA:
                builder = GetData.fromRecord((IGetDataRequest) record);
                break;
            case MULTI:
                builder = Multi.fromRecord((IMultiRequest) record);
                break;
            case SET_ACL:
                builder = SetAcl.fromRecord((ISetACLRequest) record);
                break;
            case SET_DATA:
                builder = SetData.fromRecord((ISetDataRequest) record);
                break;
            case SYNC:
                builder = Sync.fromRecord((ISyncRequest) record);
                break;
            default:
                break;
            }
            return builder;
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
    
        public static abstract class AbstractStat<T extends Records.Response, C extends AbstractStat<T,C>> extends AbstractBuilder<T> {
    
            protected Stat stat;
            
            protected AbstractStat(OpCode opcode) {
                super(opcode);
                this.stat = null;
            }
            
            protected AbstractStat(OpCode opcode, Stat stat) {
                super(opcode);
                this.stat = stat;
            }
            
            public Stat getStat() {
                return stat;
            }
            
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
    
        public static class Create extends AbstractStat<Records.Response, Create> implements PathBuilder<Records.Response> {

            public static Create fromRecord(Records.Response record) {
                OpCode opcode = record.getOpcode();
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
                OpCode opcode = record.getOpcode();
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
        
        public static class GetData extends AbstractStat<IGetDataResponse, GetData> implements DataBuilder<IGetDataResponse> {

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
                IMultiResponse record = new IMultiResponse();
                for (Builder<? extends Records.Response> e: builders) {
                    record.add((Records.MultiOpResponse) e.build());
                }
                return record;
            }
        }
        
        public static class Notification extends AbstractBuilder<IWatcherEvent> implements PathBuilder<IWatcherEvent> {

            public static Notification fromRecord(IWatcherEvent record) {
                return new Notification(
                        EventType.fromInt(record.getType()),
                        KeeperState.fromInt(record.getState()),
                        ZNodeLabel.Path.of(record.getPath()));
            }
            
            private ZNodeLabel.Path path;
            private KeeperState keeperState;
            private EventType eventType;
            
            public Notification() {
                this(EventType.None, KeeperState.SyncConnected, ZNodeLabel.Path.root());
            }

            public Notification(
                    EventType eventType,
                    KeeperState keeperState,
                    ZNodeLabel.Path path) {
                super(OpCode.NOTIFICATION);
                this.eventType = eventType;
                this.keeperState = keeperState;
                this.path = path;
            }
            
            @Override
            public ZNodeLabel.Path getPath() {
                return path;
            }

            @Override
            public Notification setPath(ZNodeLabel.Path path) {
                this.path = path;
                return this;
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
    
        public static class Sync extends AbstractBuilder<ISyncResponse> implements PathBuilder<ISyncResponse> {

            public static Sync fromRecord(ISyncResponse record) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(record.getPath());
                return new Sync(path);
            }
            
            protected ZNodeLabel.Path path;
            
            public Sync() {
                super(OpCode.SYNC);
                this.path = ZNodeLabel.Path.root();
            }

            public Sync(ZNodeLabel.Path path) {
                super(OpCode.SYNC);
                this.path = path;
            }

            @Override
            public ZNodeLabel.Path getPath() {
                return path;
            }

            @Override
            public Sync setPath(ZNodeLabel.Path path) {
                this.path = path;
                return this;
            }
    
            @Override
            public ISyncResponse build() {
                return new ISyncResponse(getPath().toString());
            }
        }

        public static AbstractBuilder<? extends Records.Response> fromRecord(Records.Response record) {
            AbstractBuilder<? extends Records.Response> builder = null;
            switch (record.getOpcode()) {
            case CHECK:
                builder = Check.fromRecord((ICheckVersionResponse) record);
                break;
            case CREATE:
            case CREATE2:
                builder = Create.fromRecord(record);
                break;
            case DELETE:
                builder = Delete.getInstance();
                break;
            case EXISTS:
                builder = Exists.fromRecord((IExistsResponse) record);
                break;
            case GET_ACL:
                builder = GetAcl.fromRecord((IGetACLResponse) record);
                break;
            case GET_CHILDREN:
            case GET_CHILDREN2:
                builder = GetChildren.fromRecord(record);
                break;
            case GET_DATA:
                builder = GetData.fromRecord((IGetDataResponse) record);
                break;
            case MULTI:
                builder = Multi.fromRecord((IMultiResponse) record);
                break;
            case NOTIFICATION:
                builder = Notification.fromRecord((IWatcherEvent) record);
                break;
            case SET_ACL:
                builder = SetAcl.fromRecord((ISetACLResponse) record);
                break;
            case SET_DATA:
                builder = SetData.fromRecord((ISetDataResponse) record);
                break;
            case SYNC:
                builder = Sync.fromRecord((ISyncResponse) record);
                break;
            default:
                break;
            }
            return builder;
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

    public static Records.Response unlessError(Records.Response reply) throws KeeperException {
        return Operations.unlessError(reply, "Unexpected Error");
    }

    public static Records.Response unlessError(Records.Response reply, String message) throws KeeperException {
        if (reply instanceof Operation.Error) {
            KeeperException.Code error = ((Operation.Error) reply).getError();
            throw KeeperException.create(error, message);
        } else {
            return reply;
        }
    }

    public static Operation.Error expectError(Records.Response reply, KeeperException.Code expected) throws KeeperException {
        return Operations.expectError(reply, expected, "Expected Error " + expected.toString());
    }

    public static Operation.Error expectError(Records.Response reply, KeeperException.Code expected, String message) throws KeeperException {
        if (reply instanceof Operation.Error) {
            Operation.Error errorReply = (Operation.Error) reply;
            KeeperException.Code error = errorReply.getError();
            if (expected != error) {
                throw KeeperException.create(error, message);
            }
            return errorReply;
        } else {
            throw new IllegalArgumentException("Unexpected Response " + reply.toString());
        }
    }

    public static Records.Response maybeError(Records.Response reply, KeeperException.Code expected) throws KeeperException {
        return Operations.maybeError(reply, expected, "Expected Error " + expected.toString());
    }

    public static Records.Response maybeError(Records.Response reply, KeeperException.Code expected, String message) throws KeeperException {
        if (reply instanceof Operation.Error) {
            KeeperException.Code error = ((Operation.Error) reply).getError();
            if (expected != error) {
                throw KeeperException.create(error, message);
            }
        }
        return reply;
    }

    private Operations() {}
}
