package edu.uw.zookeeper.client.random;

import java.util.Random;

import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.data.ZNodeCache;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.StampedValue;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Stats;

public class RandomRequestGenerator implements Generator<Records.Request> {

    public static RandomRequestGenerator fromCache(
            LockableZNodeCache<?,?,?> cache) {
        Random random = new Random();
        RandomLabel labels = RandomLabel.create(random, 1, 9);
        RandomData datum = RandomData.create(random, 0, 1024);
        CachedPaths paths = CachedPaths.fromCache(cache, random);
        RandomFromList<OpCode> opcodes = RandomFromList.create(random, BASIC_OPCODES);
        return new RandomRequestGenerator(
                random, opcodes, paths, labels, datum, cache);
    }
    
    protected static final ImmutableList<OpCode> BASIC_OPCODES = ImmutableList.of(
            OpCode.CREATE,
            OpCode.GET_CHILDREN,
            OpCode.DELETE, 
            OpCode.EXISTS, 
            OpCode.GET_DATA, 
            OpCode.SET_DATA, 
            OpCode.SYNC);
    
    protected final Random random;
    protected final ZNodeCache<?,?,?> cache;
    protected final Generator<OpCode> opcodes;
    protected final Generator<ZNodePath> paths;
    protected final Generator<ZNodeLabel> labels;
    protected final Generator<byte[]> datum;

    protected RandomRequestGenerator(
            Random random, 
            Generator<OpCode> opcodes,
            Generator<ZNodePath> paths,
            Generator<ZNodeLabel> labels, 
            Generator<byte[]> datum, 
            ZNodeCache<?,?,?> client) {
        this.random = random;
        this.opcodes = opcodes;
        this.labels = labels;
        this.datum = datum;
        this.cache = client;
        this.paths = paths;
    }
    
    @Override
    public synchronized Records.Request next() {
        Operations.Builder<? extends Records.Request> builder;
        
        synchronized (cache.cache()) {
            ZNodePath path;
            ZNodeCache.CacheNode<?,?> node = null;
            do {
                path = nextPath();
                node = cache.cache().get(path);
            } while (node == null);
            
            StampedValue<Records.ZNodeStatGetter> statView = node.stat();
            Records.ZNodeStatGetter stat = (statView == null) ? null : statView.get();
            int version = (stat == null) ? Stats.VERSION_ANY : stat.getVersion();
            
            OpCode opcode;
            while (true) {
                opcode = opcodes.next();
                if (opcode == OpCode.DELETE) {
                    if (path.isRoot() || !node.isEmpty()) {
                        continue;
                    }
                } else if ((opcode == OpCode.CREATE) || (opcode == OpCode.CREATE2)) {
                    if ((stat == null) 
                            || (stat.getEphemeralOwner() != Stats.CreateStat.ephemeralOwnerNone())) {
                        continue;
                    }
                }
                break;
            }
            
            builder = Operations.Requests.fromOpCode(opcode);
            if (builder instanceof Operations.Requests.Create) {
                CreateMode mode = CreateMode.values()[random.nextInt(CreateMode.values().length)];
                ZNodeLabel child = labels.next();
                while (node.containsKey(child)) {
                    child = labels.next();
                }
                ((Operations.Requests.Create) builder).setPath(path.join(child)).setMode(mode).setData(datum.next());          
            } else {
                ((Operations.PathBuilder<?,?>) builder).setPath(path);
    
                if (builder instanceof Operations.Requests.VersionBuilder<?,?>) {
                    ((Operations.Requests.VersionBuilder<?,?>) builder).setVersion(version);
                }
                if (builder instanceof Operations.Requests.WatchBuilder<?,?>) {
                    ((Operations.Requests.WatchBuilder<?,?>) builder).setWatch(random.nextBoolean());
                }
                if (builder instanceof Operations.DataBuilder<?,?>) {
                    ((Operations.DataBuilder<?,?>) builder).setData(datum.next());
                }
            }
        }

        return builder.build();
    }
    
    protected ZNodePath nextPath() {
        ZNodePath path;
        do {
            path = paths.next();
        } while (path.startsWith(AbsoluteZNodePath.zookeeper()));
        return path;
    }
}
