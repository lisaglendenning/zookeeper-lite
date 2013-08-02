package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.StampedReference;
import edu.uw.zookeeper.data.Stats;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class RandomCacheOperationClient<T extends Operation.ProtocolRequest<Records.Request>, V extends Operation.ProtocolResponse<Records.Response>> implements Callable<ListenableFuture<Pair<T,V>>> {

    public static <T extends Operation.ProtocolRequest<Records.Request>, V extends Operation.ProtocolResponse<Records.Response>> RandomCacheOperationClient<T,V> create(
            ZNodeViewCache<?,? super Records.Request,T,V> client) {
        Random random = new Random();
        RandomLabel labels = RandomLabel.create(random, Range.closedOpen(1, 9));
        RandomData datum = RandomData.create(random, Range.closedOpen(0, 1024));
        RandomOperation operations = RandomOperation.create(random, labels, datum, client);
        return new RandomCacheOperationClient<T,V>(operations, client);
    }
    
    protected static final ImmutableSet<OpCode> BASIC_OPCODE_SET = 
            ImmutableSet.copyOf(
                    EnumSet.of(        
                            OpCode.CHECK, 
                            OpCode.CREATE, 
                            OpCode.CREATE2,
                            OpCode.GET_CHILDREN, 
                            OpCode.GET_CHILDREN2, 
                            OpCode.DELETE, 
                            OpCode.EXISTS, 
                            OpCode.GET_DATA, 
                            OpCode.SET_DATA, 
                            OpCode.SYNC));
    
    protected static final OpCode[] BASIC_OPCODES = BASIC_OPCODE_SET.toArray(new OpCode[0]);
    
    protected final RandomOperation operations;
    protected final ZNodeViewCache<?,? super Records.Request,T,V> client;
    
    public RandomCacheOperationClient(
            RandomOperation operations,
            ZNodeViewCache<?,? super Records.Request,T,V> client) {
        this.operations = operations;
        this.client = client;
    }
    
    @Override
    public ListenableFuture<Pair<T,V>> call() {
        return client.submit(operations.next());
    }
    
    public static abstract class Randomizer<T> {

        protected final Random random;
        
        protected Randomizer(Random random) {
            this.random = random;
        }
        
        public abstract T next();
    }
    
    public static abstract class RandomArray<T> extends Randomizer<T> {

        protected final int minLength;
        protected final int lengthRange;
        
        protected RandomArray(Random random, Range<Integer> lengthBounds) {
            super(random);
            this.minLength = lengthBounds.lowerEndpoint();
            this.lengthRange = lengthBounds.upperEndpoint() - minLength;
        }
        
        public int nextLength() {
            return random.nextInt(lengthRange) + minLength;
        }
    }

    public static class RandomLabel extends RandomArray<ZNodeLabel.Component> {
    
        public static RandomLabel create(Random random, Range<Integer> lengthBounds) {
            return new RandomLabel(random, lengthBounds, ALPHABET);
        }
        
        // no easy way to get a dictionary?
        public static final String LOWER_ALPHA = "abcdefghijklmnopqrstuvwxyz";
        public static final String UPPER_ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        public static final String DIGITS = "1234567890";
        public static final String SYMBOLS = "._- ";
        public static final char[] ALPHABET = 
                new StringBuilder()
                .append(LOWER_ALPHA)
                .append(UPPER_ALPHA)
                .append(DIGITS)
                .append(SYMBOLS)
                .toString().toCharArray();
        
        protected final char[] alphabet;
        
        public RandomLabel(Random random, Range<Integer> lengthBounds, char[] alphabet) {
            super(random, lengthBounds);
            checkArgument(minLength > 0);
            checkArgument(lengthRange > 0);
            this.alphabet = alphabet;
        }
        
        @Override
        public ZNodeLabel.Component next() {
            int length = nextLength();
            char[] chars = new char[length];
            for (int i=0; i<length; ++i) {
                chars[i] = alphabet[random.nextInt(alphabet.length)];
            }
            return ZNodeLabel.Component.of(String.valueOf(chars));
        }
    }
    
    public static class RandomData extends RandomArray<byte[]> {

        public static RandomData create(Random random, Range<Integer> lengthBounds) {
            return new RandomData(random, lengthBounds);
        }
        
        public RandomData(Random random, Range<Integer> lengthBounds) {
            super(random, lengthBounds);
        }

        @Override
        public byte[] next() {
            int length = nextLength();
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            return bytes;
        }
    }
    
    public static class RandomOperation extends Randomizer<Records.Request> {

        public static RandomOperation create(
                Random random, 
                Randomizer<ZNodeLabel.Component> labels, 
                Randomizer<byte[]> datum, 
                ZNodeViewCache<?,?,?,?> client) {
            return new RandomOperation(random, labels, datum, client);
        }
        
        protected final ZNodeViewCache<?,?,?,?> client;
        protected final Set<ZNodeLabel.Path> paths;
        protected final Randomizer<ZNodeLabel.Component> labels;
        protected final Randomizer<byte[]> datum;
        
        public RandomOperation(
                Random random, 
                Randomizer<ZNodeLabel.Component> labels, 
                Randomizer<byte[]> datum, 
                ZNodeViewCache<?,?,?,?> client) {
            super(random);
            this.labels = labels;
            this.datum = datum;
            this.client = client;
            this.paths = Collections.synchronizedSet(Sets.<ZNodeLabel.Path>newHashSet());

            client.register(this);
            for (ZNodeViewCache.NodeCache<?> e: client.trie()) {
                paths.add(e.path());
            }
        }
        
        @Override
        public synchronized Records.Request next() {
            assert (! paths.isEmpty());
            ZNodeLabel.Path path = Iterables.get(paths, random.nextInt(paths.size()));
            ZNodeViewCache.NodeCache<?> node = client.trie().get(path);
            while (node == null) {
                path = Iterables.get(paths, random.nextInt(paths.size()));
                node = client.trie().get(path);
            }
            StampedReference<Records.StatGetter> statView = node.asView(ZNodeViewCache.View.STAT);
            Records.StatGetter stat = (statView == null) ? null : statView.get();
            int version = (stat == null) ? Stats.VERSION_ANY : stat.getStat().getVersion();
            OpCode opcode;
            while (true) {
                opcode = BASIC_OPCODES[random.nextInt(BASIC_OPCODES.length)];
                if (opcode == OpCode.DELETE) {
                    if (path.isRoot() || !client.trie().get(path).isEmpty()) {
                        continue;
                    }
                } else if ((opcode == OpCode.CREATE) || (opcode == OpCode.CREATE2)) {
                    if ((stat == null) 
                            || (stat.getStat().getEphemeralOwner() != Stats.CreateStat.ephemeralOwnerNone())) {
                        continue;
                    }
                }
                break;
            }
            Operations.Builder<? extends Records.Request> builder = Operations.Requests.fromOpCode(opcode);
            if (builder instanceof Operations.Requests.Create) {
                CreateMode mode = CreateMode.values()[random.nextInt(CreateMode.values().length)];
                ZNodeLabel.Component child = labels.next();
                while (node.containsKey(child)) {
                    child = labels.next();
                }
                ((Operations.Requests.Create) builder).setPath(ZNodeLabel.Path.of(path, child)).setMode(mode).setData(datum.next());          
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

            return builder.build();
        }

        @Subscribe
        public synchronized void handleNodeUpdate(ZNodeViewCache.NodeUpdate event) {
            switch (event.type()) {
            case NODE_ADDED:
                paths.add(event.path().get());
                break;
            case NODE_REMOVED:
                paths.remove(event.path().get());
                break;
            }
        }
    }
}
