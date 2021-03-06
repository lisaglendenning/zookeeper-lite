package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.proto.*;
import edu.uw.zookeeper.server.ByOpcodeTxnRequestProcessor;

public class ZNodeNode extends AbstractNameTrie.SimpleNode<ZNodeNode> {

    public static ZNodeNode root() {
        return root(ZNodeState.defaults());
    }
    
    public static ZNodeNode root(ZNodeState state) {
        NameTrie.Pointer<ZNodeNode> pointer = SimpleLabelTrie.strongPointer(EmptyZNodeLabel.getInstance(), null);
        return new ZNodeNode(
                pointer, 
                state);
    }
    
    public static ZNodeNode child(ZNodeLabel label, ZNodeNode parent, ZNodeState state) {
        NameTrie.Pointer<ZNodeNode> pointer = SimpleLabelTrie.weakPointer(label, parent);
        return new ZNodeNode(
                pointer, 
                state);
    }

    private final ZNodeState state;
    
    protected ZNodeNode(NameTrie.Pointer<? extends ZNodeNode> parent, ZNodeState state) {
        super(SimpleLabelTrie.pathOf(parent), parent, Maps.<ZNodeName, ZNodeNode>newHashMap());
        this.state = state;
    }

    public ZNodeState state() {
        return state;
    }
    
    public Stats.ImmutableStat asStat() {
        int dataLength = state.getData().getDataLength();
        int numChildren = size();
        return Stats.ImmutableStat.copyOf(Stats.CompositeStatGetter.of(
                state.asStatPersisted(), dataLength, numChildren));
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper("")
                .add("path", path())
                .add("children", keySet())
                .add("state", state())
                .toString();
    }
    
    public static interface Operator<V extends Records.Response> extends Reference<NameTrie<ZNodeNode>>, Processors.CheckedProcessor<TxnOperation.Request<?>, V, KeeperException> {
    }

    public static abstract class AbstractProcessor<V> implements Processors.CheckedProcessor<TxnOperation.Request<?>, V, KeeperException>, Reference<NameTrie<ZNodeNode>> {

        public static ZNodePath getPath(Records.PathGetter record) throws KeeperException.BadArgumentsException {
            try {
                return ZNodePath.validated(record.getPath());
            } catch (IllegalArgumentException e) {
                throw new KeeperException.BadArgumentsException(record.getPath());
            }
        }
        
        public static ZNodeNode getNode(NameTrie<ZNodeNode> trie, ZNodeName path) throws KeeperException.NoNodeException {
            ZNodeNode node = trie.get(path);
            if (node == null) {
                throw new KeeperException.NoNodeException(path.toString());
            }
            return node;
        }
        
        protected final NameTrie<ZNodeNode> trie;
        
        protected AbstractProcessor(NameTrie<ZNodeNode> trie) {
            this.trie = trie;
        }
        
        @Override
        public NameTrie<ZNodeNode> get() {
            return trie;
        }
    }
    
    public static abstract class AbstractCreate<V> extends AbstractProcessor<V> {

        protected AbstractCreate(NameTrie<ZNodeNode> trie) {
            super(trie);
        }
        
        @Override
        public V apply(TxnOperation.Request<?> request) throws KeeperException {
            Records.CreateModeGetter record = (Records.CreateModeGetter) request.record();
            ZNodePath path = getPath(record);
            if (path.isRoot()) {
                throw new KeeperException.BadArgumentsException(path.toString());
            }
            CreateMode mode = CreateMode.valueOf(record.getFlags());
            if (! mode.contains(CreateFlag.SEQUENTIAL) && get().containsKey(path)) {
                throw new KeeperException.NodeExistsException(path.toString());
            }
            ZNodeName parentPath = ((AbsoluteZNodePath) path).parent();
            ZNodeNode parent = getNode(get(), parentPath);
            if (parent.state().getCreate().isEphemeral()) {
                throw new KeeperException.NoChildrenForEphemeralsException(parentPath.toString());
            }
            
            return doCreate(request, record, mode, parent, (AbsoluteZNodePath) path);
        }
        
        protected abstract V doCreate(
                TxnOperation.Request<?> request,
                Records.CreateModeGetter record,
                CreateMode mode,
                ZNodeNode parent,
                AbsoluteZNodePath path);
    }

    @Operational(OpCode.DELETE)
    public static abstract class AbstractDelete<V> extends AbstractProcessor<V> {
    
        protected AbstractDelete(NameTrie<ZNodeNode> trie) {
            super(trie);
        }
    
        @Override
        public V apply(TxnOperation.Request<?> request)
                throws KeeperException {
            IDeleteRequest record = (IDeleteRequest) request.record();
            ZNodePath path = getPath(record);
            if (path.isRoot()) {
                throw new KeeperException.BadArgumentsException(path.toString());
            }
            ZNodeNode node = getNode(get(), path);
            if (node.size() > 0) {
                throw new KeeperException.NotEmptyException(path.toString());
            }
            if (! node.state().getData().getStat().compareVersion(record.getVersion())) {
                throw new KeeperException.BadVersionException(path.toString());
            }
            ZNodeNode parent = getNode(get(), ((AbsoluteZNodePath) path).parent());
            
            return doDelete(request, record, path, node, parent);
        }
        
        protected abstract V doDelete(
                TxnOperation.Request<?> request,
                IDeleteRequest record,
                ZNodeLabelVector path,
                ZNodeNode node,
                ZNodeNode parent);
    }
    
    @Operational(OpCode.SET_DATA)
    public static abstract class AbstractSetData<V> extends AbstractProcessor<V> {

        protected AbstractSetData(NameTrie<ZNodeNode> trie) {
            super(trie);
        }

        @Override
        public V apply(TxnOperation.Request<?> request)
                throws KeeperException {
            ISetDataRequest record = (ISetDataRequest) request.record();
            ZNodeLabelVector path = getPath(record);
            ZNodeNode node = getNode(get(), path);
            if (! node.state().getData().getStat().compareVersion(record.getVersion())) {
                throw new KeeperException.BadVersionException(path.toString());
            }
            
            return doSetData(request, record, path, node);
        }
        
        protected abstract V doSetData(
                TxnOperation.Request<?> request,
                ISetDataRequest record,
                ZNodeLabelVector path,
                ZNodeNode node);
    }

    public static abstract class Operators {
        
        private Operators() {}

        public static Map<OpCode, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>> of(NameTrie<ZNodeNode> trie) {
            return of(trie, Maps.<OpCode, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>>newEnumMap(OpCode.class));
        }
        
        public static Map<OpCode, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>> of(NameTrie<ZNodeNode> trie, Map<OpCode, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>> operators) {
            for (Class<?> cls: Operators.class.getDeclaredClasses()) {
                Operational annotation = cls.getAnnotation(Operational.class);
                if (annotation == null) {
                    continue;
                }
                if (! Operator.class.isAssignableFrom(cls)) {
                    continue;
                }
                Operator<?> operator = null;
                for (Constructor<?> ctor: cls.getConstructors()) {
                    if (ctor.getParameterTypes().length == 1) {
                        try {
                            operator = (Operator<?>) ctor.newInstance(trie);
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                        break;
                    }
                }
                assert (operator != null);
                for (OpCode opcode: annotation.value()) {
                    operators.put(opcode, operator);
                }
            }
            return operators;
        }

        @Operational(OpCode.CHECK)
        public static class CheckOperator extends AbstractProcessor<ICheckVersionResponse> implements Operator<ICheckVersionResponse> {
        
            public CheckOperator(NameTrie<ZNodeNode> trie) {
                super(trie);
            }
        
            @Override
            public ICheckVersionResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                ICheckVersionRequest record = (ICheckVersionRequest) request.record();
                ZNodeLabelVector path = getPath(record);
                ZNodeNode node = getNode(get(), path);
                if (! node.state().getData().getStat().compareVersion(record.getVersion())) {
                    throw new KeeperException.BadVersionException(path.toString());
                }
                return Operations.Responses.check().setStat(node.asStat()).build();      
            }
        }

        @Operational({OpCode.CREATE, OpCode.CREATE2})
        public static class CreateOperator extends AbstractCreate<Records.Response> implements Operator<Records.Response> {
    
            public CreateOperator(NameTrie<ZNodeNode> trie) {
                super(trie);
            }
    
            @Override
            protected Records.Response doCreate(
                    TxnOperation.Request<?> request,
                    Records.CreateModeGetter record,
                    CreateMode mode,
                    ZNodeNode parent,
                    AbsoluteZNodePath path) {
                int cversion = parent.state().getChildren().getAndIncrement(request.zxid());
                if (mode.contains(CreateFlag.SEQUENTIAL)) {
                    path = AbsoluteZNodePath.fromString(Sequential.fromInt(path, cversion).toString());
                }
                
                long ephemeralOwner = mode.contains(CreateFlag.EPHEMERAL) ? request.getSessionId() : Stats.CreateStat.ephemeralOwnerNone();
                Stats.CreateStat createStat = Stats.CreateStat.of(request.zxid(), request.getTime(), ephemeralOwner);
                byte[] bytes = record.getData();
                bytes = (bytes == null) ? ZNodeData.emptyBytes() : bytes;
                ZNodeData data = ZNodeData.of(Stats.DataStat.initialVersion(request.zxid(), request.getTime()), bytes);
                ZNodeAcl acl = ZNodeAcl.initialVersion(Acls.Acl.fromRecordList(record.getAcl()));
                Stats.ChildrenStat childrenStat = Stats.ChildrenStat.initialVersion(request.zxid());
                ZNodeState state = ZNodeState.of(createStat, data, acl, childrenStat);
                ZNodeLabel label = path.label();
                ZNodeNode node = ZNodeNode.child(label, parent, state);
                parent.put(label, node);
                Operations.Responses.Create builder = 
                        Operations.Responses.create().setPath(path);
                if (OpCode.CREATE2 == request.record().opcode()) {
                    builder.setStat(node.asStat());
                }
                return builder.build();
            }
        }

        @Operational(OpCode.DELETE)
        public static class DeleteOperator extends AbstractDelete<IDeleteResponse> implements Operator<IDeleteResponse> {
    
            public DeleteOperator(NameTrie<ZNodeNode> trie) {
                super(trie);
            }
    
            @Override
            protected IDeleteResponse doDelete(
                    TxnOperation.Request<?> request,
                    IDeleteRequest record,
                    ZNodeLabelVector path,
                    ZNodeNode node,
                    ZNodeNode parent) {
                get().remove(path);
                parent.state().getChildren().getAndIncrement(request.zxid());
                return Operations.Responses.delete().build();
            }
        }

        @Operational(OpCode.EXISTS)
        public static class ExistsOperator extends AbstractProcessor<IExistsResponse> implements Operator<IExistsResponse> {
    
            public ExistsOperator(NameTrie<ZNodeNode> trie) {
                super(trie);
            }
    
            @Override
            public IExistsResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                IExistsRequest record = (IExistsRequest) request.record();
                ZNodeLabelVector path = getPath(record);
                ZNodeNode node = getNode(get(), path);
                return Operations.Responses.exists().setStat(node.asStat()).build();
            }
        }

        @Operational(OpCode.GET_DATA)
        public static class GetDataOperator extends AbstractProcessor<IGetDataResponse> implements Operator<IGetDataResponse> {
    
            public GetDataOperator(NameTrie<ZNodeNode> trie) {
                super(trie);
            }
    
            @Override
            public IGetDataResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                IGetDataRequest record = (IGetDataRequest) request.record();
                ZNodeLabelVector path = getPath(record);
                ZNodeNode node = getNode(get(), path);
                return Operations.Responses.getData().setData(node.state().getData().getData()).setStat(node.asStat()).build();
            }
        }

        @Operational(OpCode.SET_DATA)
        public static class SetDataOperator extends AbstractSetData<ISetDataResponse> implements Operator<ISetDataResponse> {
    
            public SetDataOperator(NameTrie<ZNodeNode> trie) {
                super(trie);
            }
    
            @Override
            protected ISetDataResponse doSetData(
                    TxnOperation.Request<?> request,
                    ISetDataRequest record,
                    ZNodeLabelVector path,
                    ZNodeNode node) {
                node.state().getData().getStat().getAndIncrement(request.zxid(), request.getTime());
                byte[] bytes = record.getData();
                bytes = (bytes == null) ? ZNodeData.emptyBytes() : bytes;
                node.state().getData().setData(bytes);
                return Operations.Responses.setData().setStat(node.asStat()).build();
            }
        }

        @Operational(OpCode.GET_ACL)
        public static class GetAclOperator extends AbstractProcessor<IGetACLResponse> implements Operator<IGetACLResponse> {
    
            public GetAclOperator(NameTrie<ZNodeNode> trie) {
                super(trie);
            }
    
            @Override
            public IGetACLResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                IGetACLRequest record = (IGetACLRequest) request.record();
                ZNodeLabelVector path = getPath(record);
                ZNodeNode node = getNode(get(), path);
                return Operations.Responses.getAcl().setAcl(node.state().getAcl().getAcl()).setStat(node.asStat()).build();
            }
        }

        @Operational(OpCode.SET_ACL)
        public static class SetAclOperator extends AbstractProcessor<ISetACLResponse> implements Operator<ISetACLResponse> {
    
            public SetAclOperator(NameTrie<ZNodeNode> trie) {
                super(trie);
            }
    
            @Override
            public ISetACLResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                ISetACLRequest record = (ISetACLRequest) request.record();
                ZNodeLabelVector path = getPath(record);
                ZNodeNode node = getNode(get(), path);
                if (! node.state().getAcl().compareVersion(record.getVersion())) {
                    throw new KeeperException.BadVersionException(path.toString());
                }
                node.state().getAcl().getAndIncrement();
                node.state().getAcl().setAcl(Acls.Acl.fromRecordList(record.getAcl()));
                return Operations.Responses.setAcl().setStat(node.asStat()).build();        
            }
        }

        @Operational({OpCode.GET_CHILDREN, OpCode.GET_CHILDREN2})
        public static class GetChildrenOperator extends AbstractProcessor<Records.Response> implements Operator<Records.Response> {
    
            public GetChildrenOperator(NameTrie<ZNodeNode> trie) {
                super(trie);
            }
    
            @SuppressWarnings({ "unchecked", "rawtypes" })
            @Override
            public Records.Response apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                ZNodeLabelVector path = getPath((Records.PathGetter) request.record());
                ZNodeNode node = getNode(get(), path);
                Operations.Responses.GetChildren builder = Operations.Responses.getChildren();
                builder.setChildren(ImmutableList.<ZNodeLabel>copyOf((Set) node.keySet()));
                if (OpCode.GET_CHILDREN2 == request.record().opcode()) {
                    builder.setStat(node.asStat());
                }
                return builder.build();       
            }
        }

        @Operational(OpCode.SYNC)
        public static class SyncOperator extends AbstractProcessor<ISyncResponse> implements Operator<ISyncResponse> {
    
            public SyncOperator(NameTrie<ZNodeNode> trie) {
                super(trie);
            }
    
            @Override
            public ISyncResponse apply(TxnOperation.Request<?> request)
                    throws KeeperException {
                ISyncRequest record = (ISyncRequest) request.record();
                ZNodePath path = getPath(record);
                getNode(get(), path);
                return Operations.Responses.sync().setPath(path).build();        
            }
        }
    }
    
    public static abstract class AbstractUndo implements Processors.UncheckedProcessor<Records.Response, Void> {

        protected final TxnOperation.Request<?> request;
        protected final NameTrie<ZNodeNode> trie;
        
        protected AbstractUndo(
                TxnOperation.Request<?> request,
                NameTrie<ZNodeNode> trie) {
            this.request = request;
            this.trie = trie;
        }
    }
    
    @Operational({OpCode.CREATE, OpCode.CREATE2})    
    public static class CreateUndo extends AbstractUndo {
        
        protected final Stats.ChildrenStat parentStat;
        
        public CreateUndo(
                Stats.ChildrenStat parentStat,
                TxnOperation.Request<?> request,
                NameTrie<ZNodeNode> trie) {
            super(request, trie);
            this.parentStat = parentStat;
        }
        
        @Override
        public Void apply(Records.Response result) {
            ZNodeNode node = trie.get(((Records.PathGetter) result).getPath());
            ZNodeNode parent = node.parent().get();
            node.remove();
            parent.state().setChildren(parentStat);
            return null;
        }
    }
    
    @Operational(OpCode.DELETE)    
    public static class DeleteUndo extends AbstractUndo {
        
        protected final Stats.ChildrenStat parentStat;
        protected final ZNodeState state;
        
        public DeleteUndo(
                Stats.ChildrenStat parentStat,
                ZNodeState state,
                TxnOperation.Request<?> request,
                NameTrie<ZNodeNode> trie) {
            super(request, trie);
            this.parentStat = parentStat;
            this.state = state;
        }

        @Override
        public Void apply(Records.Response result) {
            AbsoluteZNodePath path = AbsoluteZNodePath.fromString(((Records.PathGetter) request.record()).getPath());
            ZNodeNode parent = trie.get(path.parent());
            ZNodeLabel label = path.label();
            ZNodeNode node = ZNodeNode.child(label, parent, state);
            parent.put(label, node);
            return null;
        }
    }

    @Operational(OpCode.SET_DATA)    
    public static class SetDataUndo extends AbstractUndo {
        
        protected final ZNodeData data;
        
        public SetDataUndo(
                ZNodeData data,
                TxnOperation.Request<?> request,
                NameTrie<ZNodeNode> trie) {
            super(request, trie);
            this.data = data;
        }

        @Override
        public Void apply(Records.Response result) {
            ZNodeNode node = trie.get(((Records.PathGetter) request.record()).getPath());
            node.state().getData().set(data);
            return null;
        }
    }
    
    @Operational({OpCode.CREATE, OpCode.CREATE2})
    public static class CreateCopyState extends AbstractCreate<CreateUndo> {

        public CreateCopyState(NameTrie<ZNodeNode> trie) {
            super(trie);
        }

        @Override
        protected CreateUndo doCreate(
                TxnOperation.Request<?> request,
                Records.CreateModeGetter record,
                CreateMode mode,
                ZNodeNode parent,
                AbsoluteZNodePath path) {
            return new CreateUndo(
                    Stats.ChildrenStat.copyOf(parent.state().getChildren()),
                    request,
                    get());
        }
    }

    @Operational(OpCode.DELETE)
    public static class DeleteCopyState extends AbstractDelete<DeleteUndo> {

        public DeleteCopyState(NameTrie<ZNodeNode> trie) {
            super(trie);
        }

        @Override
        protected DeleteUndo doDelete(
                TxnOperation.Request<?> request,
                IDeleteRequest record,
                ZNodeLabelVector path,
                ZNodeNode node,
                ZNodeNode parent) {
            return new DeleteUndo(
                    Stats.ChildrenStat.copyOf(parent.state().getChildren()),
                    ZNodeState.copyOf(node.state()),
                    request,
                    get());
        }
    }

    @Operational(OpCode.SET_DATA)
    public static class SetDataCopyState extends AbstractSetData<SetDataUndo> {

        public SetDataCopyState(NameTrie<ZNodeNode> trie) {
            super(trie);
        }

        @Override
        protected SetDataUndo doSetData(
                TxnOperation.Request<?> request,
                ISetDataRequest record,
                ZNodeLabelVector path,
                ZNodeNode node) {
            return new SetDataUndo(
                    ZNodeData.copyOf(node.state().getData()),
                    request,
                    get());
        }
    }

    @Operational(OpCode.MULTI)        
    public static class MultiOperator extends AbstractProcessor<IMultiResponse> implements Operator<IMultiResponse> {

        public static MultiOperator of(NameTrie<ZNodeNode> trie) {
            return of(trie, ByOpcodeTxnRequestProcessor.create(ImmutableMap.copyOf(Operators.of(trie))));
        }
        
        public static MultiOperator of(NameTrie<ZNodeNode> trie, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException> delegate) {
            return new MultiOperator(trie, delegate);
        }
        
        protected final Map<OpCode, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends AbstractUndo, KeeperException>> copiers;
        protected final Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException> delegate;
        
        protected MultiOperator(
                NameTrie<ZNodeNode> trie,
                Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException> delegate) {
            super(trie);
            this.delegate = delegate;
            List<Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends AbstractUndo, KeeperException>> copiers = 
                    ImmutableList.<Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends AbstractUndo, KeeperException>>of(
                            new CreateCopyState(trie),
                            new DeleteCopyState(trie),
                            new SetDataCopyState(trie));
            ImmutableMap.Builder<OpCode, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends AbstractUndo, KeeperException>> builder = ImmutableMap.builder();
            for (Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends AbstractUndo, KeeperException> copier: copiers) {
                for (OpCode code: copier.getClass().getAnnotation(Operational.class).value()) {
                    builder.put(code, copier);
                }
            }
            this.copiers = builder.build();
        }

        @Override
        public IMultiResponse apply(TxnOperation.Request<?> input)
                throws KeeperException {
            IMultiRequest record = (IMultiRequest) input.record();
            IErrorResponse error = null;
            List<AbstractUndo> undos = Lists.newArrayListWithCapacity(record.size());
            List<Records.MultiOpResponse> results = Lists.newArrayListWithCapacity(record.size());
            for (Records.MultiOpRequest request: record) {
                Records.MultiOpResponse result = null;
                if (error != null) {
                    result = error;
                } else {
                    Message.ClientRequest<?> nestedRequest = ProtocolRequestMessage.of(
                            input.xid(), request);
                    TxnOperation.Request<?> nested = TxnRequest.of(
                            input.getTime(), 
                            input.zxid(), 
                            SessionRequest.of(input.getSessionId(), nestedRequest));
                    AbstractUndo undo = null;
                    try {
                        switch (request.opcode()) {
                        case CREATE:
                        case CREATE2:
                        case DELETE:
                        case SET_DATA:
                            undo = copiers.get(request.opcode()).apply(nested);
                            break;
                        case CHECK:
                            break;
                        default:
                            throw new AssertionError(String.valueOf(request));
                        }
                        
                        result = (Records.MultiOpResponse) delegate.apply(nested);
                        
                        undos.add(undo);
                    } catch (KeeperException e) {
                        result = Operations.Responses.error().setError(e.code()).build();
                        assert (error == null);
                        error = Operations.Responses.error().setError(KeeperException.Code.RUNTIMEINCONSISTENCY).build();
                        
                        for (int i=undos.size()-1;  i>=0;  --i) {
                            if (undos.get(i) != null) {
                                undos.get(i).apply(results.get(i));
                            }
                        }
                        
                        IErrorResponse ok = Operations.Responses.error().setError(KeeperException.Code.OK).build();
                        for (int i=0; i<undos.size();  ++i) {
                            results.set(i, ok);
                        }
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
            return of(stat, emptyBytes());
        }
        
        public static ZNodeData of(Stats.DataStat stat, byte[] data) {
            return new ZNodeData(stat, data);
        }
        
        public static ZNodeData copyOf(ZNodeData value) {
            return of(Stats.DataStat.copyOf(value.getStat()),
                    (value.getData() == null) ? null 
                            : Arrays.copyOf(value.getData(), 
                                    value.getData().length));
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
        
        public void set(ZNodeData value) {
            setData(value.getData());
            stat.set(value.getStat());
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("dataLength", getDataLength())
                    .add("stat", getStat())
                    .toString();
        }
    }
    
    public static class ZNodeAcl implements Records.AclStatSetter {

        public static ZNodeAcl anyoneAll() {
            return initialVersion(Acls.Definition.ANYONE_ALL.asList());
        }

        public static ZNodeAcl initialVersion(List<Acls.Acl> acl) {
            return of(acl, Stats.initialVersion());
        }

        public static ZNodeAcl of(List<Acls.Acl> acl, int aversion) {
            return new ZNodeAcl(acl, aversion);
        }

        public static ZNodeAcl copyOf(ZNodeAcl value) {
            return of((value.getAcl() == null) ? null 
                    : Lists.newArrayList(value.getAcl()), 
                    value.getAversion());
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
            return MoreObjects.toStringHelper(this)
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
            return of(
                    Stats.CreateStat.nonEphemeral(zxid, time),
                    ZNodeData.empty(Stats.DataStat.initialVersion(zxid, time)),
                    ZNodeAcl.anyoneAll(),
                    Stats.ChildrenStat.initialVersion(zxid));
        }

        public static ZNodeState copyOf(ZNodeState value) {
            return of(Stats.CreateStat.copyOf(value.getCreate()),
                    ZNodeData.copyOf(value.getData()),
                    ZNodeAcl.copyOf(value.getAcl()),
                    Stats.ChildrenStat.copyOf(value.getChildren()));
        }
                
        public static ZNodeState of(
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
        
        public void setData(ZNodeData data) {
            this.data.set(data);
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
        
        public void setChildren(Stats.ChildrenStat children) {
            this.children.set(children);
        }
        
        public Stats.CompositeStatPersistedGetter asStatPersisted() {
            return Stats.CompositeStatPersistedGetter.of(
                    getCreate(), getData().getStat(), getAcl(), getChildren());
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("create", getCreate())
                    .add("children", getChildren())
                    .add("data", getData())
                    .add("acl", getData())
                    .toString();
        }
    }
}
