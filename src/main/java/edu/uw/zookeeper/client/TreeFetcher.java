package edu.uw.zookeeper.client;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.ChildrenRecord;
import edu.uw.zookeeper.protocol.proto.Records.OpCodeXid;

public class TreeFetcher implements Callable<List<WatchEvent>> {
    
    public static class QueueWatcher {

        protected final BlockingQueue<WatchEvent> queue;

        public QueueWatcher() {
            this(new LinkedBlockingQueue<WatchEvent>());
        }
        
        public QueueWatcher(BlockingQueue<WatchEvent> queue) {
            this.queue = queue;
        }
        
        public BlockingQueue<WatchEvent> queue() {
            return queue;
        }
        
        @Subscribe
        public void handleReply(Operation.SessionReply message) throws InterruptedException {
            int xid = message.xid();
            if (OpCodeXid.has(xid) && OpCodeXid.NOTIFICATION == OpCodeXid.of(xid)) {
                IWatcherEvent record = (IWatcherEvent) ((Operation.RecordHolder<?>)message.reply()).asRecord();
                handleEvent(record);
            }
        }
        
        public void handleEvent(IWatcherEvent record) throws InterruptedException {
            WatchEvent event = WatchEvent.of(record);
            queue.put(event);
        }
    }
    
    public static class SubtreeWatcher extends TreeFetcher.QueueWatcher {

        protected final ZNodeLabel.Path root;

        public SubtreeWatcher(ZNodeLabel.Path root) {
            super();
            this.root = root;
        }
        
        public SubtreeWatcher(ZNodeLabel.Path root, BlockingQueue<WatchEvent> queue) {
            super(queue);
            this.root = root;
        }

        @Override
        public void handleEvent(IWatcherEvent record) throws InterruptedException {
            ZNodeLabel.Path path = ZNodeLabel.Path.of(record.getPath());
            if (root.prefixOf(path)) {
                super.handleEvent(record);
            }
        }
    }

    public static List<WatchEvent> fetchTree(ZNodeLabel.Path root, ClientExecutor client, Set<OpCode> operations, boolean watch) throws InterruptedException, ExecutionException, KeeperException {
        boolean getData = operations.contains(OpCode.GET_DATA);
        boolean getAcl = operations.contains(OpCode.GET_ACL);
        boolean getStat = operations.contains(OpCode.EXISTS) 
                || operations.contains(OpCode.GET_CHILDREN2);
        
        TreeFetcher.SubtreeWatcher watcher;
        if (watch) {
            watcher = new SubtreeWatcher(root);
            client.register(watcher);
        } else {
            watcher = null;
        }
        
        LinkedList<ZNodeLabel.Path> paths = Lists.newLinkedList();
        paths.add(root);
        while (! paths.isEmpty()) {
            ZNodeLabel.Path next = paths.poll();
            Operations.Requests.Builder.GetChildren getChildrenBuilder = 
                    Operations.Requests.getChildren().setPath(next).setWatch(watch);
            if (getStat) {
                getChildrenBuilder.setStat(true);
            }
            Operation.Request request = getChildrenBuilder.build();
            Operation.SessionResult result = client.submit(request).get();
            Operation.Reply reply = Operation.maybeError(result.reply().reply(), KeeperException.Code.NONODE, request.toString());
            if (reply instanceof Operation.Response) {
                Records.ChildrenRecord responseRecord = (ChildrenRecord) ((Operation.RecordHolder<?>)reply).asRecord();
                for (String child: responseRecord.getChildren()) {
                    paths.add(ZNodeLabel.Path.of(next, ZNodeLabel.Component.of(child)));
                }
                
                if (getData) {
                    Operations.Requests.Builder.GetData getDataBuilder = 
                            Operations.Requests.getData().setPath(next).setWatch(watch);
                    request = getDataBuilder.build();
                    result = client.submit(request).get();
                    reply = Operation.maybeError(result.reply().reply(), KeeperException.Code.NONODE, request.toString());
                }
                
                if (getAcl) {
                    Operations.Requests.Builder.GetAcl getAclBuilder = 
                            Operations.Requests.getAcl().setPath(next);
                    request = getAclBuilder.build();
                    result = client.submit(request).get();
                    reply = Operation.maybeError(result.reply().reply(), KeeperException.Code.NONODE, request.toString());
                }
            }
        }
        
        List<WatchEvent> events;
        if (watch) {
            client.unregister(watcher);
            events = Lists.newLinkedList();
            watcher.queue().drainTo(events);
        } else {
            events = ImmutableList.of();
        }
        return events;
    }   
    
    protected volatile ZNodeLabel.Path root;
    protected volatile ClientExecutor client;
    protected volatile Set<OpCode> operations; 
    protected volatile boolean watch;
    
    public TreeFetcher() {
        this(ZNodeLabel.Path.root(), null, EnumSet.noneOf(OpCode.class), false);
    }
    
    public TreeFetcher(ZNodeLabel.Path root, ClientExecutor client, Set<OpCode> operations, boolean watch) {
        this.root = root;
        this.client = client;
        this.watch = watch;
        this.operations = Collections.synchronizedSet(operations);
    }
    
    public ZNodeLabel.Path getRoot() {
        return root;
    }

    public TreeFetcher setRoot(ZNodeLabel.Path root) {
        this.root = root;
        return this;
    }
    
    public ClientExecutor getClient() {
        return client;
    }

    public TreeFetcher setClient(ClientExecutor client) {
        this.client = client;
        return this;
    }

    public boolean getStat() {
        OpCode[] ops = { OpCode.EXISTS, OpCode.GET_CHILDREN2 };
        for (OpCode op: ops) {
            if (operations.contains(op)) {
                return true;
            }
        }
        return false;
    }
    
    public TreeFetcher setStat(boolean getStat) {
        OpCode[] ops = { OpCode.EXISTS, OpCode.GET_CHILDREN2 };
        if (getStat) {
            for (OpCode op: ops) {
                operations.add(op);
            }
        } else {
            for (OpCode op: ops) {
                operations.remove(op);
            }
        }
        return this;
    }

    public boolean getData() {
        OpCode op = OpCode.GET_DATA;
        return operations.contains(op);
    }
    
    public TreeFetcher setData(boolean getData) {
        OpCode op = OpCode.GET_DATA;
        if (getData) {
            operations.add(op);
        } else {
            operations.remove(op);
        }
        return this;
    }

    public boolean getAcl() {
        OpCode op = OpCode.GET_ACL;
        return operations.contains(op);
    }
    
    public TreeFetcher setAcl(boolean getAcl) {
        OpCode op = OpCode.GET_ACL;
        if (getAcl) {
            operations.add(op);
        } else {
            operations.remove(op);
        }
        return this;
    }
    
    public boolean getWatch() {
        return watch;
    }
    
    public TreeFetcher setWatch(boolean watch) {
        this.watch = watch;
        return this;
    }
    
    @Override
    public List<WatchEvent> call() throws Exception {
        return fetchTree(root, client, operations, watch);
    }
}