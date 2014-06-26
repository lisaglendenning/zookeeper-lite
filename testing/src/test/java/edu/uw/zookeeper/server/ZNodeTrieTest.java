package edu.uw.zookeeper.server;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.ZNodeTrieExecutor;
import edu.uw.zookeeper.client.SessionClientExecutor;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.ZNodeCache;
import edu.uw.zookeeper.client.SubmitGenerator;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.CountingGenerator;
import edu.uw.zookeeper.client.random.RandomRequestGenerator;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

@RunWith(JUnit4.class)
public class ZNodeTrieTest {
    
    protected final Logger logger = LogManager.getLogger();
    
    @Test(timeout=10000)
    public void testRandom() throws Exception {
        ZNodeTrieExecutor executor = ZNodeTrieExecutor.defaults();
        LockableZNodeCache<ZNodeCache.SimpleCacheNode, Records.Request, Message.ServerResponse<?>> cache = 
                LockableZNodeCache.newInstance(SessionClientExecutor.create(1, executor));
        int iterations = 100;
        Generator<Records.Request> requests = RandomRequestGenerator.fromCache(cache);
        CountingGenerator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> operations = CountingGenerator.create(
                iterations, iterations, SubmitGenerator.create(requests, cache), logger);
        while (operations.hasNext()) {
             Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> operation = operations.next();
             assertFalse(operation.second().get(1000, TimeUnit.MILLISECONDS).record() instanceof Operation.Error);
        }
    }
}
