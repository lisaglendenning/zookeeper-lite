package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.OpRecord;
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

public abstract class Operations {

    public static abstract class Requests {
    
        public static abstract class Builder<T extends Records.RequestRecord> {

            protected OpCode opcode;
            
            protected Builder(OpCode opcode) {
                this.opcode = opcode;
            }
            
            public abstract T build();
            
            public abstract static class Path<T extends Records.RequestRecord, C extends Path<T,C>> extends Builder<T> {

                protected ZNodeLabel.Path path;
                
                protected Path(OpCode opcode) {
                    super(opcode);
                    this.path = ZNodeLabel.Path.root();
                }
                
                public ZNodeLabel.Path getPath() {
                    return path;
                }
                
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
            
            public abstract static class Data<T extends Records.RequestRecord, C extends Data<T,C>> extends Path<T,C> {
                protected byte[] data;
                
                protected Data(OpCode opcode) {
                    super(opcode);
                    this.data = new byte[0];
                }
                
                public byte[] getData() {
                    return data;
                }
                
                @SuppressWarnings("unchecked")
                public C setData(byte[] data) {
                    this.data = checkNotNull(data);
                    return (C) this;
                }
            }
            
            public static class Create extends Data<Records.CreateRecord, Create> {

                protected CreateMode mode;
                protected List<ACL> acl;
                
                public Create() {
                    super(OpCode.CREATE);
                    this.mode = CreateMode.PERSISTENT;
                    this.acl = ImmutableList.of();
                }
                
                public CreateMode getMode() {
                    return mode;
                }
                
                public Create setMode(CreateMode mode) {
                    this.mode = checkNotNull(mode);
                    return this;
                }
                
                public boolean hasStat() {
                    return (opcode == OpCode.CREATE2);
                }
                
                /**
                 * >= 3.5.0
                 */
                public Create getStat() {
                    this.opcode = OpCode.CREATE2;
                    return this;
                }
                
                public List<ACL> getAcl() {
                    return acl;
                }
                
                public Create setAcl(List<ACL> acl) {
                    this.acl = checkNotNull(acl);
                    return this;
                }
                
                @Override
                public Records.CreateRecord build() {
                    Records.CreateRecord record = OpRecord.OpRequest.newRecord(opcode);
                    record.setAcl(getAcl());
                    record.setPath(getPath().toString());
                    record.setData(getData());
                    record.setFlags(getMode().toFlag());
                    return record;
                }
            }
            
            public static class Delete extends Path<IDeleteRequest, Delete> {
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

            public static class Exists extends Path<IExistsRequest, Exists> {

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
            
            public static class GetAcl extends Path<IGetACLRequest, GetAcl> {
                public GetAcl() {
                    super(OpCode.GET_ACL);
                }
            }
            
            public static class GetChildren extends Path<Records.RequestRecord, GetChildren> {

                protected boolean watch;
                
                public GetChildren() {
                    super(OpCode.GET_CHILDREN);
                }

                public boolean getWatch() {
                    return watch;
                }
                
                public GetChildren setWatch(boolean watch) {
                    this.watch = watch;
                    return this;
                }

                public boolean hasStat() {
                    return (opcode == OpCode.GET_CHILDREN2);
                }
                
                public GetChildren getStat() {
                    this.opcode = OpCode.GET_CHILDREN2;
                    return this;
                }
                
                @Override
                public Records.RequestRecord build() {
                    Records.RequestRecord record = OpRecord.OpRequest.newRecord(opcode);
                    ((Records.PathRecord)record).setPath(getPath().toString());
                    ((Records.WatchRecord)record).setWatch(getWatch());
                    return record;
                }
            }

            public static class GetData extends Path<IGetDataRequest, GetData> {

                protected boolean watch;
                
                public GetData() {
                    super(OpCode.GET_DATA);
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
            
            public static class Multi extends Builder<IMultiRequest> implements Iterable<Builder<? extends Records.MultiOpRequest>> {

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

            public static class SetAcl extends Path<ISetACLRequest, SetAcl> {

                protected int version;
                protected List<ACL> acl;
                
                public SetAcl() {
                    super(OpCode.SET_ACL);
                    this.version = -1;
                    this.acl = ImmutableList.of();
                }
                
                public int getVersion() {
                    return version;
                }
                
                public SetAcl setVersion(int version) {
                    this.version = version;
                    return this;
                }
                
                public List<ACL> getAcl() {
                    return acl;
                }
                
                public SetAcl setAcl(List<ACL> acl) {
                    this.acl = checkNotNull(acl);
                    return this;
                }
                
                @Override
                public ISetACLRequest build() {
                    ISetACLRequest record = new ISetACLRequest(
                            getPath().toString(), getAcl(), getVersion());
                    return record;
                }
            }

            public static class SetData extends Data<ISetDataRequest, SetData> {

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
            
            public static class Sync extends Path<ISyncRequest, Sync> {
                public Sync() {
                    super(OpCode.SYNC);
                }
            }
        }
        
        public static Builder.Create create() {
            return new Builder.Create();
        }
        
        public static Builder.Delete delete() {
            return new Builder.Delete();
        }

        public static Builder.Exists exists() {
            return new Builder.Exists();
        }

        public static Builder.GetAcl getAcl() {
            return new Builder.GetAcl();
        }
        
        public static Builder.GetChildren getChildren() {
            return new Builder.GetChildren();
        }

        public static Builder.GetData getData() {
            return new Builder.GetData();
        }
        
        public static Builder.Multi multi() {
            return new Builder.Multi();
        }

        public static Builder.SetAcl setAcl() {
            return new Builder.SetAcl();
        }

        public static Builder.SetData setData() {
            return new Builder.SetData();
        }
        
        public static Builder.Sync sync() {
            return new Builder.Sync();
        }
        
        private Requests() {}
    }
    
    private Operations() {}
}
