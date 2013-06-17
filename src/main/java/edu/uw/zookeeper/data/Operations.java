package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.*;

public abstract class Operations {

    public static interface Builder<T extends Operation.Action> {

        OpCode getOpCode();
        
        T build();
    }
    
    public static abstract class AbstractBuilder<T extends Operation.Action> implements Builder<T> {

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
    
    public static interface PathBuilder<T extends Operation.Action> extends Builder<T> {
        ZNodeLabel.Path getPath();
        PathBuilder<T> setPath(ZNodeLabel.Path path);
    }

    public static interface DataBuilder<T extends Operation.Action> extends Builder<T> {
        byte[] getData();
        DataBuilder<T> setData(byte[] data);
    }
    
    public static abstract class Requests {
        
        public static abstract class AbstractPath<T extends Operation.Request, C extends AbstractPath<T,C>> extends AbstractBuilder<T> implements PathBuilder<T> {
    
            protected ZNodeLabel.Path path;
            
            protected AbstractPath(OpCode opcode) {
                super(opcode);
                this.path = ZNodeLabel.Path.root();
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

        public static abstract class AbstractData<T extends Operation.Request, C extends AbstractData<T,C>> extends AbstractPath<T,C> implements DataBuilder<T> {
            protected byte[] data;
            
            protected AbstractData(OpCode opcode) {
                super(opcode);
                this.data = new byte[0];
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
        
        public static class Create extends AbstractData<Operation.Request, Create> {

            protected CreateMode mode;
            protected List<Acls.Acl> acls;
            
            public Create() {
                super(OpCode.CREATE);
                this.mode = CreateMode.PERSISTENT;
                this.acls = Acls.Definition.ANYONE_ALL.asList();
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
                return acls;
            }
            
            public Create setAcl(List<Acls.Acl> acls) {
                this.acls = checkNotNull(acls);
                return this;
            }
            
            @Override
            public Operation.Request build() {
                Operation.Request record = getStat() 
                        ? new ICreate2Request(getPath().toString(), getData(), Acls.Acl.asRecordList(getAcl()), getMode().toFlag()) 
                        : new ICreateRequest(getPath().toString(), getData(), Acls.Acl.asRecordList(getAcl()), getMode().toFlag());
                return record;
            }
        }
        
        public static class Delete extends AbstractPath<IDeleteRequest, Delete> {
            protected int version;
            
            public Delete() {
                super(OpCode.DELETE);
                this.version = -1;
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

            protected boolean watch;
            
            public Exists() {
                super(OpCode.EXISTS);
                this.watch = false;
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
            public GetAcl() {
                super(OpCode.GET_ACL);
            }

            @Override
            public IGetACLRequest build() {
                return new IGetACLRequest(getPath().toString());
            }
        }
        
        public static class GetChildren extends AbstractPath<Operation.Request, GetChildren> {

            protected boolean watch;
            
            public GetChildren() {
                super(OpCode.GET_CHILDREN);
                this.watch = false;
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
            public Operation.Request build() {
                Operation.Request record = getStat()
                        ? new IGetChildren2Request(getPath().toString(), getWatch())
                        : new IGetChildrenRequest(getPath().toString(), getWatch());
                return record;
            }
        }

        public static class GetData extends AbstractPath<IGetDataRequest, GetData> {

            protected boolean watch;
            
            public GetData() {
                super(OpCode.GET_DATA);
                this.watch = false;
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
        
        public static class Multi extends AbstractBuilder<IMultiRequest> implements Iterable<Builder<? extends Records.MultiOpRequest>> {

            protected List<Builder<? extends Records.MultiOpRequest>> builders;
            
            public Multi() {
                super(OpCode.MULTI);
                this.builders = Lists.newArrayList();
            }
            
            public Multi add(Builder<? extends Records.MultiOpRequest> builder) {
                builders.add(builder);
                return this;
            }

            @Override
            public Iterator<Builder<? extends Records.MultiOpRequest>> iterator() {
                return builders.iterator();
            }
            
            @Override
            public IMultiRequest build() {
                IMultiRequest record = new IMultiRequest();
                for (Builder<? extends Records.MultiOpRequest> builder: builders) {
                    record.add(builder.build());
                }
                return record;
            }
        }

        public static class SetAcl extends AbstractPath<ISetACLRequest, SetAcl> {

            protected int version;
            protected List<Acls.Acl> acls;
            
            public SetAcl() {
                super(OpCode.SET_ACL);
                this.version = -1;
                this.acls = Acls.Definition.ANYONE_ALL.asList();
            }
            
            public int getVersion() {
                return version;
            }
            
            public SetAcl setVersion(int version) {
                this.version = version;
                return this;
            }
            
            public List<Acls.Acl> getAcl() {
                return acls;
            }
            
            public SetAcl setAcl(List<Acls.Acl> acls) {
                this.acls = checkNotNull(acls);
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

            protected int version;
            
            public SetData() {
                super(OpCode.SET_DATA);
                this.version = -1;
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
            public Sync() {
                super(OpCode.SYNC);
            }

            @Override
            public ISyncRequest build() {
                return new ISyncRequest(getPath().toString());
            }
        }
        
        public static class SerializedData<T extends Operation.Request, C extends AbstractData<T,C>, V> extends AbstractBuilder<T> implements PathBuilder<T>, DataBuilder<T> {

            public static <T extends Operation.Request, C extends AbstractData<T,C>, V> SerializedData<T,C,V> of (C delegate, Serializers.ByteSerializer<? super V> serializer, V input) {
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
        
        public static <T extends Operation.Request, C extends AbstractData<T,C>, V> SerializedData<T,C,V> serialized(C delegate, Serializers.ByteSerializer<? super V> serializer, V input) {
            return SerializedData.of(delegate, serializer, input);
        }
        
        private Requests() {}
    }
    
    public static abstract class Responses {
    
        public static abstract class AbstractStat<T extends Operation.Response, C extends AbstractStat<T,C>> extends AbstractBuilder<T> {
    
            protected Stat stat;
            
            protected AbstractStat(OpCode opcode) {
                super(opcode);
                this.stat = null;
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
        
        public static class Create extends AbstractStat<Operation.Response, Create> implements PathBuilder<Operation.Response> {
    
            protected ZNodeLabel.Path path;
            
            public Create() {
                super(OpCode.CREATE);
                this.path = ZNodeLabel.Path.root();
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
            public Operation.Response build() {
                Operation.Response record = (getOpCode() == OpCode.CREATE2)
                        ? new ICreate2Response(getPath().toString(), getStat())
                        : new ICreateResponse(getPath().toString());
                return record;
            }
        }
        
        public static enum Delete implements Builder<IDeleteResponse> {
            INSTANCE;
    
            @Override
            public OpCode getOpCode() {
                return OpCode.DELETE;
            }
    
            @Override
            public IDeleteResponse build() {
                return Records.create(IDeleteResponse.class);
            }
        }
    
        public static class Exists extends AbstractStat<IExistsResponse, Exists> {
    
            public Exists() {
                super(OpCode.EXISTS);
            }
            
            @Override
            public IExistsResponse build() {
                return new IExistsResponse(getStat());
            }
        }
    
        public static class GetAcl extends AbstractStat<IGetACLResponse, GetAcl> {
    
            protected List<Acls.Acl> acl;
            
            public GetAcl() {
                super(OpCode.GET_ACL);
                this.acl = ImmutableList.of();
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
        
        public static class GetChildren extends AbstractStat<Operation.Response, GetChildren> {
    
            protected Iterable<ZNodeLabel.Component> children;
            
            public GetChildren() {
                super(OpCode.GET_CHILDREN);
                this.children = ImmutableList.of();
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
            public Operation.Response build() {
                List<String> children = ImmutableList.copyOf(Iterables.transform(getChildren(), Functions.toStringFunction()));
                Operation.Response record = (OpCode.GET_CHILDREN2 == getOpCode())
                        ? new IGetChildren2Response(children, getStat())
                        : new IGetChildrenResponse(children);
                return record;
            }
        }
        
        public static class GetData extends AbstractStat<IGetDataResponse, GetData> implements DataBuilder<IGetDataResponse> {
    
            protected byte[] data;
            
            public GetData() {
                super(OpCode.GET_DATA);
                this.data = new byte[0];
                this.stat = Stats.ImmutableStat.uninitialized();
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
    
        public static class Multi extends AbstractBuilder<IMultiResponse> implements Iterable<Builder<? extends Records.MultiOpResponse>> {
    
            protected List<Builder<? extends Records.MultiOpResponse>> builders;
            
            public Multi() {
                super(OpCode.MULTI);
                this.builders = Lists.newArrayList();
            }
            
            public Multi add(Builder<? extends Records.MultiOpResponse> builder) {
                builders.add(builder);
                return this;
            }
    
            @Override
            public Iterator<Builder<? extends Records.MultiOpResponse>> iterator() {
                return builders.iterator();
            }
            
            @Override
            public IMultiResponse build() {
                IMultiResponse record = new IMultiResponse();
                for (Builder<? extends Records.MultiOpResponse> builder: builders) {
                    record.add(builder.build());
                }
                return record;
            }
        }
    
        public static class SetAcl extends AbstractStat<ISetACLResponse, SetAcl> {
    
            public SetAcl() {
                super(OpCode.SET_ACL);
            }
            
            @Override
            public ISetACLResponse build() {
                return new ISetACLResponse(getStat());
            }
        }
    
        public static class SetData extends AbstractStat<ISetDataResponse, SetData> {
    
            public SetData() {
                super(OpCode.SET_DATA);
            }
            
            @Override
            public ISetDataResponse build() {
                return new ISetDataResponse(getStat());
            }
        }
    
        public static class Sync extends AbstractBuilder<ISyncResponse> implements PathBuilder<ISyncResponse> {
    
            protected ZNodeLabel.Path path;
            
            public Sync() {
                super(OpCode.SYNC);
                this.path = ZNodeLabel.Path.root();
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
        
        public static Create create() {
            return new Create();
        }
        
        public static Delete delete() {
            return Delete.INSTANCE;
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
    }

    public static Operation.Response unlessError(Operation.Response reply) throws KeeperException {
        return Operations.unlessError(reply, "Unexpected Error");
    }

    public static Operation.Response unlessError(Operation.Response reply, String message) throws KeeperException {
        if (reply instanceof Operation.Error) {
            KeeperException.Code error = ((Operation.Error) reply).error();
            throw KeeperException.create(error, message);
        } else {
            return reply;
        }
    }

    public static Operation.Error expectError(Operation.Response reply, KeeperException.Code expected) throws KeeperException {
        return Operations.expectError(reply, expected, "Expected Error " + expected.toString());
    }

    public static Operation.Error expectError(Operation.Response reply, KeeperException.Code expected, String message) throws KeeperException {
        if (reply instanceof Operation.Error) {
            Operation.Error errorReply = (Operation.Error) reply;
            KeeperException.Code error = errorReply.error();
            if (expected != error) {
                throw KeeperException.create(error, message);
            }
            return errorReply;
        } else {
            throw new IllegalArgumentException("Unexpected Response " + reply.toString());
        }
    }

    public static Operation.Response maybeError(Operation.Response reply, KeeperException.Code expected) throws KeeperException {
        return Operations.maybeError(reply, expected, "Expected Error " + expected.toString());
    }

    public static Operation.Response maybeError(Operation.Response reply, KeeperException.Code expected, String message) throws KeeperException {
        if (reply instanceof Operation.Error) {
            KeeperException.Code error = ((Operation.Error) reply).error();
            if (expected != error) {
                throw KeeperException.create(error, message);
            }
        }
        return reply;
    }

    private Operations() {}
}
