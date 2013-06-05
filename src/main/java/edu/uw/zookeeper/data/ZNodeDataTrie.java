package edu.uw.zookeeper.data;

import java.util.List;
import java.util.Locale;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.data.ZNodeLabel.Path;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.proto.ISetACLRequest;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.protocol.proto.IStat;
import edu.uw.zookeeper.util.Pair;

public class ZNodeDataTrie extends ZNodeLabelTrie<ZNodeDataTrie.ZNodeStateNode> {

    public static class Operator {
        
        protected final ZNodeDataTrie trie;
        
        public Operator(ZNodeDataTrie trie) {
            this.trie = trie;
        }
        
        public ZNodeDataTrie trie() {
            return trie;
        }
        
        // TODO: watches
        public Records.ResponseRecord apply(Records.RequestRecord request) throws KeeperException {
            long session = 0;
            long zxid = 0;
            long time = Stats.getTime();
            
            Records.ResponseRecord response;
            switch (request.opcode()) {
            case CREATE:
            case CREATE2:
            {
                Records.CreateRecord record = (Records.CreateRecord) request;
                ZNodeLabel.Path path = ZNodeLabel.Path.of(record.getPath());
                ZNodeLabel parentPath = path.head();
                ZNodeStateNode parent = trie.get(parentPath);
                if (parent == null) {
                    throw new KeeperException.NoNodeException(parentPath.toString());
                }
                if (parent.state().getCreateStat().isEphemeral()) {
                    throw new KeeperException.NoChildrenForEphemeralsException(parentPath.toString());
                }
                Pair<Integer, Long> cversion = parent.state().getChildrenStat().getAndIncrement(zxid);

                CreateMode mode = CreateMode.fromFlag(record.getFlags());
                if (mode.isSequential()) {
                    path = Path.of(path.toString() + String.format(Locale.ENGLISH, "%010d", cversion.first()));
                } else {
                    if (trie.get(path) != null) {
                        throw new KeeperException.NodeExistsException(path.toString());
                    }
                }
                long ephemeralOwner = mode.isEphemeral() ? session : Stats.CreateStat.ephemeralOwnerNone();
                Stats.CreateStat createStat = Stats.CreateStat.of(zxid, time, ephemeralOwner);
                ZNodeData data = ZNodeData.newInstance(Stats.DataStat.newInstance(zxid, time), record.getData());
                ZNodeAcl acl = ZNodeAcl.newInstance(Acls.Acl.fromRecordList(record.getAcl()));
                Stats.ChildrenStat childrenStat = Stats.ChildrenStat.newInstance(zxid);
                ZNodeState state = ZNodeState.newInstance(createStat, data, acl, childrenStat);
                
                ZNodeStateNode node = parent.put(path.tail(), state);
                Operations.Responses.Create builder = 
                        Operations.Responses.create().setPath(path);
                if (OpCode.CREATE2 == request.opcode()) {
                    builder.setStat(node.asStat());
                }
                response = builder.build();
                break;
            }
            case DELETE:
            {
                ZNodeLabel.Path path = Path.of(((Records.PathHolder) request).getPath());
                ZNodeStateNode node = trie.get(path);
                if (node == null) {
                    throw new KeeperException.NoNodeException(path.toString());
                }
                if (node.size() > 0) {
                    throw new KeeperException.NotEmptyException(path.toString());
                }
                trie.remove(path);
                response = Operations.Responses.delete().build();
                break;
            }
            case EXISTS:
            {
                ZNodeLabel.Path path = Path.of(((Records.PathHolder) request).getPath());
                ZNodeStateNode node = trie.get(path);
                if (node == null) {
                    throw new KeeperException.NoNodeException(path.toString());
                }
                response = Operations.Responses.exists().setStat(node.asStat()).build();
                break;
            }
            case GET_DATA:
            {
                ZNodeLabel.Path path = Path.of(((Records.PathHolder) request).getPath());
                ZNodeStateNode node = trie.get(path);
                if (node == null) {
                    throw new KeeperException.NoNodeException(path.toString());
                }
                response = Operations.Responses.getData().setData(node.state().getData().getData()).setStat(node.asStat()).build();
                break;
            }
            case SET_DATA:
            {
                ISetDataRequest record = (ISetDataRequest)request;
                ZNodeLabel.Path path = Path.of(((Records.PathHolder) request).getPath());
                ZNodeStateNode node = trie.get(path);
                if (node == null) {
                    throw new KeeperException.NoNodeException(path.toString());
                }
                if (! node.state().getData().getStat().compareVersion(record.getVersion())) {
                    throw new KeeperException.BadVersionException(path.toString());
                }
                node.state().getData().getStat().getAndIncrement(zxid, time);
                node.state().getData().setData(record.getData());
                response = Operations.Responses.setData().setStat(node.asStat()).build();
                break;
            }
            case GET_ACL:
            {
                ZNodeLabel.Path path = Path.of(((Records.PathHolder) request).getPath());
                ZNodeStateNode node = trie.get(path);
                if (node == null) {
                    throw new KeeperException.NoNodeException(path.toString());
                }
                response = Operations.Responses.getAcl().setAcl(node.state().getAcl().getAcl()).setStat(node.asStat()).build();
                break;
            }
            case SET_ACL:
            {
                ISetACLRequest record = (ISetACLRequest)request;
                ZNodeLabel.Path path = Path.of(((Records.PathHolder) request).getPath());
                ZNodeStateNode node = trie.get(path);
                if (node == null) {
                    throw new KeeperException.NoNodeException(path.toString());
                }
                if (! node.state().getAcl().compareVersion(record.getVersion())) {
                    throw new KeeperException.BadVersionException(path.toString());
                }
                node.state().getAcl().getAndIncrement();
                node.state().getAcl().setAcl(Acls.Acl.fromRecordList(record.getAcl()));
                response = Operations.Responses.setAcl().setStat(node.asStat()).build();
                break;
            }
            case GET_CHILDREN:
            case GET_CHILDREN2:
            {
                ZNodeLabel.Path path = Path.of(((Records.PathHolder) request).getPath());
                ZNodeStateNode node = trie.get(path);
                if (node == null) {
                    throw new KeeperException.NoNodeException(path.toString());
                }
                Operations.Responses.GetChildren builder = Operations.Responses.getChildren();
                builder.setChildren(ImmutableList.copyOf(node.keySet()));
                if (OpCode.GET_CHILDREN2 == request.opcode()) {
                    builder.setStat(node.asStat());
                }
                response = builder.build();
                break;
            }
            case SYNC:
            {
                ZNodeLabel.Path path = Path.of(((Records.PathHolder) request).getPath());
                ZNodeStateNode node = trie.get(path);
                if (node == null) {
                    throw new KeeperException.NoNodeException(path.toString());
                }
                response = Operations.Responses.sync().setPath(path).build();
                break;
            }
            case CHECK:
            case MULTI:
                // TODO
            default:
                throw new IllegalArgumentException(request.toString());
            }
            return response;
        }
    }
    
    public static class ZNodeData {
        
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
            this.data = data;
        }
        
        public Stats.DataStat getStat() {
            return stat;
        }
        
        public int getDataLength() {
            return data.length;
        }
    }
    
    public static class ZNodeAcl implements Records.AclStatRecord {
        
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
            this.acl = acl;
        }

        public List<Acls.Acl> getAcl() {
            return acl;
        }
        
        public void setAcl(List<Acls.Acl> acl) {
            this.acl = acl;
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
    }

    public static class ZNodeState {
        
        public static ZNodeState newInstance() {
            return newInstance(
                    Stats.CreateStat.of(0, 0, 0),
                    new ZNodeData(Stats.DataStat.of(0, 0, 0), new byte[0]),
                    new ZNodeAcl(ImmutableList.<Acls.Acl>of(), 0),
                    Stats.ChildrenStat.of(0, 0));
        }

        public static ZNodeState newInstance(
                Stats.CreateStat createStat,
                ZNodeData data,
                ZNodeAcl acl,
                Stats.ChildrenStat childrenStat) {
            return new ZNodeState(createStat, data, acl, childrenStat);
        }

        protected final Stats.CreateStat createStat;
        protected final ZNodeData data;
        protected final ZNodeAcl acl;
        protected final Stats.ChildrenStat childrenStat;
        
        public ZNodeState(
                Stats.CreateStat createStat,
                ZNodeData data,
                ZNodeAcl acl,
                Stats.ChildrenStat childrenStat) {
            this.createStat = createStat;
            this.data = data;
            this.acl = acl;
            this.childrenStat = childrenStat;
        }
        
        public ZNodeData getData() {
            return data;
        }
        
        public Stats.CreateStat getCreateStat() {
            return createStat;
        }
        
        public ZNodeAcl getAcl() {
            return acl;
        }
        
        public Stats.ChildrenStat getChildrenStat() {
            return childrenStat;
        }
    }
    
    public static class ZNodeStateNode extends ZNodeLabelTrie.AbstractNode<ZNodeStateNode> {

        public static ZNodeStateNode root(ZNodeState state) {
            return new ZNodeStateNode(Optional.<Pointer<ZNodeStateNode>>absent(), state);
        }
        
        public static ZNodeStateNode child(ZNodeLabel.Component label, ZNodeStateNode parent, ZNodeState state) {
            Pointer<ZNodeStateNode> pointer = SimplePointer.of(label, parent);
            return new ZNodeStateNode(Optional.of(pointer), state);
        }

        protected final ZNodeState state;
        
        protected ZNodeStateNode(Optional<Pointer<ZNodeStateNode>> parent, ZNodeState state) {
            super(parent);
            this.state = state;
        }
        
        public ZNodeState state() {
            return state;
        }
        
        public ZNodeStateNode put(ZNodeLabel.Component label, ZNodeState state) {
            return put(label, child(label, this, state));
        }
        
        public IStat asStat() {
            int dataLength = state.getData().getDataLength();
            int numChildren = children.size();
            return Stats.ImmutableStat.copyOf(new Stats.CompositeStatHolder(state.getCreateStat(), state.getData().getStat(), state.getAcl(), state.getChildrenStat(), dataLength, numChildren));
        }
    }
    
    protected ZNodeDataTrie(ZNodeStateNode root) {
        super(root);
    }

}
