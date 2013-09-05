package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.proto.*;
import edu.uw.zookeeper.protocol.server.ByOpcodeTxnRequestProcessor;

public class ZNodeDataTrie extends ZNodeLabelTrie<ZNodeDataTrie.ZNodeStateNode> {

    public static ZNodeDataTrie newInstance() {
        return new ZNodeDataTrie(ZNodeStateNode.root());
    }
    
    public static interface Operator<V extends Records.Response> extends Reference<ZNodeDataTrie>, Processors.CheckedProcessor<TxnOperation.Request<?>, V, KeeperException> {
    }

    public static abstract class AbstractOperator<V extends Records.Response> implements Operator<V> {

        public static ZNodeLabel.Path getPath(Records.PathGetter record) {
            return ZNodeLabel.Path.of(record.getPath());
        }
        
        public static ZNodeStateNode getNode(ZNodeDataTrie trie, ZNodeLabel path) throws KeeperException.NoNodeException {
            ZNodeStateNode node = trie.get(path);
            if (node == null) {
                throw new KeeperException.NoNodeException(path.toString());
            }
            return node;
        }
        
        protected final ZNodeDataTrie trie;
        
        protected AbstractOperator(ZNodeDataTrie trie) {
            this.trie = trie;
        }
        
        @Override
        public ZNodeDataTrie get() {
            return trie;
        }
    }

    public static abstract class Operators {
        
        private Operators() {}

        public static Map<OpCode, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>> of(ZNodeDataTrie trie) {
            return of(trie, Maps.<OpCode, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>>newEnumMap(OpCode.class));
        }
        
        public static Map<OpCode, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>> of(ZNodeDataTrie trie, Map<OpCode, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>> operators) {
            for (Class<?> cls: Operators.class.getDeclaredClasses()) {
                Operational annotation = cls.getAnnotation(Operational.class);
                if (annotation == null) {
                    continue;
                }
                if (! Operator.class.isAssignableFrom(cls)) {
                    continue;
                }
                Operator<?> operator;
                try {
                    operator = (Operator<?>) cls.getConstructor(trie.getClass()).newInstance(trie);
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
                for (OpCode opcode: annotation.value()) {
                    operators.put(opcode, operator);
                }
            }
            return operators;
        }

        @Operational(OpCode.CHECK)
        public static class CheckOperator extends AbstractOperator<ICheckVersionResponse> {
        
            public CheckOperator(ZNodeDataTrie trie) {
                super(trie);
            }
        
            @Override
            public ICheckVersionResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                ICheckVersionRequest record = (ICheckVersionRequest) request.record();
                ZNodeLabel.Path path = getPath(record);
                ZNodeStateNode node = getNode(get(), path);
                if (! node.state().getData().getStat().compareVersion(record.getVersion())) {
                    throw new KeeperException.BadVersionException(path.toString());
                }
                return Operations.Responses.check().setStat(node.asStat()).build();      
            }
        }

        @Operational({OpCode.CREATE, OpCode.CREATE2})
        public static class CreateOperator extends AbstractOperator<Records.Response> {
    
            public CreateOperator(ZNodeDataTrie trie) {
                super(trie);
            }
    
            @Override
            public Records.Response apply(TxnOperation.Request<?> request) throws KeeperException {
                Records.CreateModeGetter record = (Records.CreateModeGetter) request.record();
                ZNodeLabel.Path path = getPath(record);
                ZNodeLabel parentPath = path.head();
                ZNodeStateNode parent = getNode(get(), parentPath);
                if (parent.state().getCreate().isEphemeral()) {
                    throw new KeeperException.NoChildrenForEphemeralsException(parentPath.toString());
                }
                CreateMode mode = CreateMode.valueOf(record.getFlags());
                if (! mode.contains(CreateFlag.SEQUENTIAL) && (get().get(path) != null)) {
                    throw new KeeperException.NodeExistsException(path.toString());
                }
                
                int cversion = parent.state().getChildren().getAndIncrement(request.zxid());
                if (mode.contains(CreateFlag.SEQUENTIAL)) {
                    path = ZNodeLabel.Path.of(Sequenced.toString(path, cversion));
                }
                
                long ephemeralOwner = mode.contains(CreateFlag.EPHEMERAL) ? request.getSessionId() : Stats.CreateStat.ephemeralOwnerNone();
                Stats.CreateStat createStat = Stats.CreateStat.of(request.zxid(), request.getTime(), ephemeralOwner);
                byte[] bytes = record.getData();
                bytes = (bytes == null) ? ZNodeData.emptyBytes() : bytes;
                ZNodeData data = ZNodeData.newInstance(Stats.DataStat.newInstance(request.zxid(), request.getTime()), bytes);
                ZNodeAcl acl = ZNodeAcl.newInstance(Acls.Acl.fromRecordList(record.getAcl()));
                Stats.ChildrenStat childrenStat = Stats.ChildrenStat.newInstance(request.zxid());
                ZNodeState state = ZNodeState.newInstance(createStat, data, acl, childrenStat);
                
                ZNodeStateNode node = parent.add((ZNodeLabel.Component) path.tail(), state);
                Operations.Responses.Create builder = 
                        Operations.Responses.create().setPath(path);
                if (OpCode.CREATE2 == request.record().opcode()) {
                    builder.setStat(node.asStat());
                }
                return builder.build();
            }
        }

        @Operational(OpCode.DELETE)
        public static class DeleteOperator extends AbstractOperator<IDeleteResponse> {
    
            public DeleteOperator(ZNodeDataTrie trie) {
                super(trie);
            }
    
            @Override
            public IDeleteResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                IDeleteRequest record = (IDeleteRequest) request.record();
                ZNodeLabel.Path path = getPath(record);
                ZNodeStateNode node = getNode(get(), path);
                if (node.size() > 0) {
                    throw new KeeperException.NotEmptyException(path.toString());
                }
                if (! node.state().getData().getStat().compareVersion(record.getVersion())) {
                    throw new KeeperException.BadVersionException(path.toString());
                }
                ZNodeStateNode parent = getNode(get(), path.head());
                get().remove(path);
                parent.state().getChildren().getAndIncrement(request.zxid());
                return Operations.Responses.delete().build();
            }
        }

        @Operational(OpCode.EXISTS)
        public static class ExistsOperator extends AbstractOperator<IExistsResponse> {
    
            public ExistsOperator(ZNodeDataTrie trie) {
                super(trie);
            }
    
            @Override
            public IExistsResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                IExistsRequest record = (IExistsRequest) request.record();
                ZNodeLabel.Path path = getPath(record);
                ZNodeStateNode node = getNode(get(), path);
                return Operations.Responses.exists().setStat(node.asStat()).build();
            }
        }

        @Operational(OpCode.GET_DATA)
        public static class GetDataOperator extends AbstractOperator<IGetDataResponse> {
    
            public GetDataOperator(ZNodeDataTrie trie) {
                super(trie);
            }
    
            @Override
            public IGetDataResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                IGetDataRequest record = (IGetDataRequest) request.record();
                ZNodeLabel.Path path = getPath(record);
                ZNodeStateNode node = getNode(get(), path);
                return Operations.Responses.getData().setData(node.state().getData().getData()).setStat(node.asStat()).build();
            }
        }

        @Operational(OpCode.SET_DATA)
        public static class SetDataOperator extends AbstractOperator<ISetDataResponse> {
    
            public SetDataOperator(ZNodeDataTrie trie) {
                super(trie);
            }
    
            @Override
            public ISetDataResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                ISetDataRequest record = (ISetDataRequest) request.record();
                ZNodeLabel.Path path = getPath(record);
                ZNodeStateNode node = getNode(get(), path);
                if (! node.state().getData().getStat().compareVersion(record.getVersion())) {
                    throw new KeeperException.BadVersionException(path.toString());
                }
                node.state().getData().getStat().getAndIncrement(request.zxid(), request.getTime());
                byte[] bytes = record.getData();
                bytes = (bytes == null) ? ZNodeData.emptyBytes() : bytes;
                node.state().getData().setData(bytes);
                return Operations.Responses.setData().setStat(node.asStat()).build();
            }
        }

        @Operational(OpCode.GET_ACL)
        public static class GetAclOperator extends AbstractOperator<IGetACLResponse> {
    
            public GetAclOperator(ZNodeDataTrie trie) {
                super(trie);
            }
    
            @Override
            public IGetACLResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                IGetACLRequest record = (IGetACLRequest) request.record();
                ZNodeLabel.Path path = getPath(record);
                ZNodeStateNode node = getNode(get(), path);
                return Operations.Responses.getAcl().setAcl(node.state().getAcl().getAcl()).setStat(node.asStat()).build();
            }
        }

        @Operational(OpCode.SET_ACL)
        public static class SetAclOperator extends AbstractOperator<ISetACLResponse> {
    
            public SetAclOperator(ZNodeDataTrie trie) {
                super(trie);
            }
    
            @Override
            public ISetACLResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                ISetACLRequest record = (ISetACLRequest) request.record();
                ZNodeLabel.Path path = getPath(record);
                ZNodeStateNode node = getNode(get(), path);
                if (! node.state().getAcl().compareVersion(record.getVersion())) {
                    throw new KeeperException.BadVersionException(path.toString());
                }
                node.state().getAcl().getAndIncrement();
                node.state().getAcl().setAcl(Acls.Acl.fromRecordList(record.getAcl()));
                return Operations.Responses.setAcl().setStat(node.asStat()).build();        
            }
        }

        @Operational({OpCode.GET_CHILDREN, OpCode.GET_CHILDREN2})
        public static class GetChildrenOperator extends AbstractOperator<Records.Response> {
    
            public GetChildrenOperator(ZNodeDataTrie trie) {
                super(trie);
            }
    
            @Override
            public Records.Response apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                ZNodeLabel.Path path = getPath((Records.PathGetter) request.record());
                ZNodeStateNode node = getNode(get(), path);
                Operations.Responses.GetChildren builder = Operations.Responses.getChildren();
                builder.setChildren(ImmutableList.copyOf(node.keySet()));
                if (OpCode.GET_CHILDREN2 == request.record().opcode()) {
                    builder.setStat(node.asStat());
                }
                return builder.build();       
            }
        }

        @Operational(OpCode.SYNC)
        public static class SyncOperator extends AbstractOperator<ISyncResponse> {
    
            public SyncOperator(ZNodeDataTrie trie) {
                super(trie);
            }
    
            @Override
            public ISyncResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                ISyncRequest record = (ISyncRequest) request.record();
                ZNodeLabel.Path path = getPath(record);
                getNode(get(), path);
                return Operations.Responses.sync().setPath(path).build();        
            }
        }
    }
    
    @Operational(OpCode.MULTI)        
    public static class MultiOperator extends AbstractOperator<IMultiResponse> {

        public static MultiOperator of(ZNodeDataTrie trie) {
            return of(trie, ByOpcodeTxnRequestProcessor.create(ImmutableMap.copyOf(Operators.of(trie))));
        }
        
        public static MultiOperator of(ZNodeDataTrie trie, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException> delegate) {
            return new MultiOperator(trie, delegate);
        }
        
        protected final Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException> delegate;
        
        protected MultiOperator(
                ZNodeDataTrie trie,
                Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException> delegate) {
            super(trie);
            this.delegate = delegate;
        }

        @Override
        public IMultiResponse apply(TxnOperation.Request<?> input)
                throws KeeperException {
            IErrorResponse error = null;
            List<Records.MultiOpResponse> results = Lists.newArrayList();
            for (Records.MultiOpRequest request: (IMultiRequest) input.record()) {
                Records.MultiOpResponse result;
                if (error != null) {
                    result = error;
                } else {
                    Operation.ProtocolRequest<?> nestedRequest = ProtocolRequestMessage.of(input.xid(), request);
                    TxnOperation.Request<?> nested = TxnRequest.of(
                            input.getTime(), 
                            input.zxid(), 
                            SessionRequest.of(input.getSessionId(), nestedRequest, nestedRequest));
                    try {
                        switch (request.opcode()) {
                        case CHECK:
                            break;
                        case CREATE:
                        case CREATE2:
                            break;
                        case DELETE:
                            break;
                        case SET_DATA:
                            break;
                        default:
                            throw new AssertionError(request.toString());
                        }
                        
                        result = (Records.MultiOpResponse) delegate.apply(nested);
                    } catch (KeeperException e) {
                        assert (error == null);
                        error = Operations.Responses.error().setError(KeeperException.Code.RUNTIMEINCONSISTENCY).build();
                        // NOTE: save partial results...
                        // FIXME: rollback effects!!
                        throw new UnsupportedOperationException();
                    }
                }
                results.add(result);
            }
            return new IMultiResponse(results);
        }
    }
    
    public static class ZNodeData {

        public static byte[] emptyBytes() {
            return new byte[0];
        }
        
        public static ZNodeData empty(Stats.DataStat stat) {
            return newInstance(stat, emptyBytes());
        }
        
        public static ZNodeData newInstance(Stats.DataStat stat, byte[] data) {
            return new ZNodeData(stat, data);
        }

        protected final Stats.DataStat stat;
        protected byte[] data;
        
        public ZNodeData(Stats.DataStat stat, byte[] data) {
            this.stat = stat;
            this.data = data;
        }

        public byte[] getData() {
            return data;
        }
        
        public void setData(byte[] data) {
            this.data = checkNotNull(data);
        }
        
        public Stats.DataStat getStat() {
            return stat;
        }
        
        public int getDataLength() {
            return data.length;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("dataLength", getDataLength())
                    .add("stat", getStat())
                    .toString();
        }
    }
    
    public static class ZNodeAcl implements Records.AclStatSetter {

        public static ZNodeAcl anyoneAll() {
            return newInstance(Acls.Definition.ANYONE_ALL.asList());
        }

        public static ZNodeAcl newInstance(List<Acls.Acl> acl) {
            return newInstance(acl, Stats.initialVersion());
        }

        public static ZNodeAcl newInstance(List<Acls.Acl> acl, int aversion) {
            return new ZNodeAcl(acl, aversion);
        }
        
        protected int aversion;
        protected List<Acls.Acl> acl;

        public ZNodeAcl(List<Acls.Acl> acl, int aversion) {
            this.aversion = aversion;
            this.acl = checkNotNull(acl);
        }

        public List<Acls.Acl> getAcl() {
            return acl;
        }
        
        public void setAcl(List<Acls.Acl> acl) {
            this.acl = checkNotNull(acl);
        }

        @Override
        public int getAversion() {
            return aversion;
        }

        @Override
        public void setAversion(int aversion) {
            this.aversion = aversion;
        }
       
        public boolean compareVersion(int version) {
            return Stats.compareVersion(version, getAversion());
        }

        public int getAndIncrement() {
            int prev = this.aversion;
            this.aversion = prev + 1;
            return prev;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("acl", getAcl())
                    .add("aversion", getAversion())
                    .toString();
        }
    }

    public static class ZNodeState {
        
        public static ZNodeState defaults() {
            return defaults(0);
        }

        public static ZNodeState defaults(long zxid) {
            long time = Stats.getTime();
            return newInstance(
                    Stats.CreateStat.nonEphemeral(zxid, time),
                    ZNodeData.empty(Stats.DataStat.newInstance(zxid, time)),
                    ZNodeAcl.anyoneAll(),
                    Stats.ChildrenStat.newInstance(zxid));
        }

        public static ZNodeState newInstance(
                Stats.CreateStat createStat,
                ZNodeData data,
                ZNodeAcl acl,
                Stats.ChildrenStat childrenStat) {
            return new ZNodeState(createStat, data, acl, childrenStat);
        }

        protected final Stats.CreateStat create;
        protected final ZNodeData data;
        protected final ZNodeAcl acl;
        protected final Stats.ChildrenStat children;
        
        public ZNodeState(
                Stats.CreateStat create,
                ZNodeData data,
                ZNodeAcl acl,
                Stats.ChildrenStat children) {
            this.create = create;
            this.data = data;
            this.acl = acl;
            this.children = children;
        }
        
        public ZNodeData getData() {
            return data;
        }
        
        public Stats.CreateStat getCreate() {
            return create;
        }
        
        public ZNodeAcl getAcl() {
            return acl;
        }
        
        public Stats.ChildrenStat getChildren() {
            return children;
        }
        
        public Stats.CompositeStatPersistedGetter asStatPersisted() {
            return Stats.CompositeStatPersistedGetter.of(
                    getCreate(), getData().getStat(), getAcl(), getChildren());
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("create", getCreate())
                    .add("children", getChildren())
                    .add("data", getData())
                    .add("acl", getData())
                    .toString();
        }
    }
    
    public static class ZNodeStateNode extends ZNodeLabelTrie.AbstractNode<ZNodeStateNode> {

        public static ZNodeStateNode root() {
            return root(ZNodeState.defaults());
        }
        
        public static ZNodeStateNode root(ZNodeState state) {
            return new ZNodeStateNode(Optional.<Pointer<ZNodeStateNode>>absent(), state);
        }
        
        public static ZNodeStateNode child(ZNodeLabel.Component label, ZNodeStateNode parent, ZNodeState state) {
            Pointer<ZNodeStateNode> pointer = SimplePointer.of(label, parent);
            return new ZNodeStateNode(Optional.of(pointer), state);
        }

        protected final ZNodeState state;
        
        protected ZNodeStateNode(Optional<Pointer<ZNodeStateNode>> parent, ZNodeState state) {
            super(parent, new ConcurrentSkipListMap<ZNodeLabel.Component, ZNodeStateNode>());
            this.state = state;
        }

        @Override
        protected ConcurrentSkipListMap<ZNodeLabel.Component, ZNodeStateNode> delegate() {
            return (ConcurrentSkipListMap<ZNodeLabel.Component, ZNodeStateNode>) children;
        }
        
        public ZNodeState state() {
            return state;
        }
        
        public ZNodeStateNode add(ZNodeLabel.Component label, ZNodeState state) {
            delegate().putIfAbsent(label, child(label, this, state));
            return get(label);
        }
        
        public Stats.ImmutableStat asStat() {
            int dataLength = state.getData().getDataLength();
            int numChildren = children.size();
            return Stats.ImmutableStat.copyOf(Stats.CompositeStatGetter.of(state.asStatPersisted(), dataLength, numChildren));
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("path", path())
                    .add("children", keySet())
                    .add("state", state())
                    .toString();
        }
    }
    
    protected ZNodeDataTrie(ZNodeStateNode root) {
        super(root);
    }
}
