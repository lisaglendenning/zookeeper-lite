package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.OpRecord;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IDeleteRequest;
import edu.uw.zookeeper.protocol.proto.IExistsRequest;
import edu.uw.zookeeper.protocol.proto.IGetACLRequest;
import edu.uw.zookeeper.protocol.proto.IGetDataRequest;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.ISetACLRequest;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.protocol.proto.ISyncRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.MultiOpRequest;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;

public abstract class Operations {

    public static abstract class Requests {
    
        public static interface Builder<T extends Records.RequestRecord> {

            OpCode getOpCode();
            
            Builder<T> setOpCode(OpCode opcode);
            
            T build();
        }
        
        public static abstract class AbstractBuilder<T extends Records.RequestRecord> implements Builder<T> {

            protected OpCode opcode;
            
            protected AbstractBuilder(OpCode opcode) {
                this.opcode = opcode;
            }
            
            @Override
            public OpCode getOpCode() {
                return opcode;
            }

            @Override
            public Builder<T> setOpCode(OpCode opcode) {
                this.opcode = opcode;
                return this;
            }
        }
        
        public static interface Path<T extends Records.RequestRecord> extends Builder<T> {

            ZNodeLabel.Path getPath();
            
            Path<T> setPath(ZNodeLabel.Path path);
        }
            
        public static abstract class AbstractPath<T extends Records.RequestRecord, C extends AbstractPath<T,C>> extends AbstractBuilder<T> implements Path<T> {

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

            @Override
            public T build() {
                T record = OpRecord.OpRequest.newRecord(opcode);
                ((Records.PathRecord)record).setPath(path.toString());
                return record;
            }
        }

        public static interface Data<T extends Records.RequestRecord> extends Path<T> {
            byte[] getData();
            Data<T> setData(byte[] data);
        }
        
        public static abstract class AbstractData<T extends Records.RequestRecord, C extends AbstractData<T,C>> extends AbstractPath<T,C> implements Data<T> {
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
        
        public static class Create extends AbstractData<Records.CreateRecord, Create> {

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
            public Records.CreateRecord build() {
                Records.CreateRecord record = OpRecord.OpRequest.newRecord(getOpCode());
                record.setAcl(Acls.Acl.asRecordList(getAcl()));
                record.setPath(getPath().toString());
                record.setData(getData());
                record.setFlags(getMode().toFlag());
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
            
            protected Exists() {
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
        }
        
        public static class GetChildren extends AbstractPath<Records.RequestRecord, GetChildren> {

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
            public Records.RequestRecord build() {
                Records.RequestRecord record = OpRecord.OpRequest.newRecord(getOpCode());
                ((Records.PathRecord)record).setPath(getPath().toString());
                ((Records.WatchRecord)record).setWatch(getWatch());
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
            
            protected Multi() {
                super(OpCode.MULTI);
                this.builders = Lists.newArrayList();
            }
            
            public Multi add(Builder<? extends Records.MultiOpRequest> builder) {
                builders.add(builder);
                return this;
            }

            @Override
            public Iterator<Builder<? extends MultiOpRequest>> iterator() {
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
        }
        
        public static class SerializedData<T extends Records.RequestRecord, C extends AbstractData<T,C>, V> extends AbstractBuilder<T> implements Data<T> {

            public static <T extends Records.RequestRecord, C extends AbstractData<T,C>, V> SerializedData<T,C,V> of (C delegate, Serializers.ByteSerializer<? super V> serializer, V input) {
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
                try {
                    return delegate().setData(serializer().toBytes(input())).build();
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
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
        
        public static <T extends Records.RequestRecord, C extends AbstractData<T,C>, V> SerializedData<T,C,V> serialized(C delegate, Serializers.ByteSerializer<? super V> serializer, V input) {
            return SerializedData.of(delegate, serializer, input);
        }
        
        private Requests() {}
    }
    
    public static Operation.Response unlessError(Operation.Reply reply) throws KeeperException {
        return Operations.unlessError(reply, "Unexpected Error");
    }

    public static Operation.Response unlessError(Operation.Reply reply, String message) throws KeeperException {
        if (reply instanceof Operation.Error) {
            KeeperException.Code error = ((Operation.Error) reply).error();
            throw KeeperException.create(error, message);
        }
        return (Operation.Response) reply;
    }

    public static Operation.Error expectError(Operation.Reply reply, KeeperException.Code expected) throws KeeperException {
        return Operations.expectError(reply, expected, "Expected Error " + expected.toString());
    }

    public static Operation.Error expectError(Operation.Reply reply, KeeperException.Code expected, String message) throws KeeperException {
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

    public static Operation.Reply maybeError(Operation.Reply reply, KeeperException.Code expected) throws KeeperException {
        return Operations.maybeError(reply, expected, "Expected Error " + expected.toString());
    }

    public static Operation.Reply maybeError(Operation.Reply reply, KeeperException.Code expected, String message) throws KeeperException {
        if (reply instanceof Operation.Error) {
            KeeperException.Code error = ((Operation.Error) reply).error();
            if (expected != error) {
                throw KeeperException.create(error, message);
            }
        }
        return reply;
    }
    
    public static class OperationChain extends PromiseTask<Function<Operation.SessionResult, Operation.Request>, List<Operation.SessionResult>> implements FutureCallback<Operation.SessionResult> {

        public static OperationChain of(
                Function<Operation.SessionResult, Operation.Request> callback,
                ClientExecutor client) {
            Promise<List<Operation.SessionResult>> promise = newPromise();
            return of(callback, client, promise);
        }
        
        public static OperationChain of(
                Function<Operation.SessionResult, Operation.Request> callback,
                ClientExecutor client, 
                Promise<List<Operation.SessionResult>> promise) {
            return new OperationChain(callback, client, promise);
        }
        
        protected final BlockingQueue<Operation.SessionResult> results;
        protected final ClientExecutor client;
        
        protected OperationChain(
                Function<Operation.SessionResult, Operation.Request> callback,
                ClientExecutor client, 
                Promise<List<Operation.SessionResult>> promise) {
            super(callback, promise);
            this.client = client;
            this.results = new LinkedBlockingQueue<Operation.SessionResult>();
        }
        
        public BlockingQueue<Operation.SessionResult> results() {
            return results;
        }
        
        public ClientExecutor client() {
            return client;
        }

        @Override
        public void onSuccess(Operation.SessionResult result) {
            try {
                results().put(result);
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
            Operation.Request request = task().apply(result);
            if (request != null) {
                ListenableFuture<Operation.SessionResult> future = client().submit(request);
                Futures.addCallback(future, this);
            } else {
                // done
                List<Operation.SessionResult> results = Lists.newLinkedList();
                this.results.drainTo(results);
                set(results);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }

    private Operations() {}
}
