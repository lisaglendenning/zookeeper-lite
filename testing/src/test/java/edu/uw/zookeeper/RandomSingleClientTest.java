package edu.uw.zookeeper;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeCache;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.SubmitGenerator;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.CountingGenerator;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.client.random.RandomRequestGenerator;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.Records;

@RunWith(JUnit4.class)
public class RandomSingleClientTest {
    
    public static <O extends Operation.ProtocolResponse<?>> LockableZNodeCache<ZNodeCache.SimpleCacheNode, Operation.Request, O> randomCache(
            int iterations,
            TimeValue timeOut,
            ClientExecutor<Operation.Request,O,SessionListener> client,
            Logger logger) throws KeeperException, InterruptedException, ExecutionException, TimeoutException {
        LockableZNodeCache<ZNodeCache.SimpleCacheNode, Operation.Request, O> cache = 
                LockableZNodeCache.newInstance( 
                        client);
        Generator<Records.Request> requests = RandomRequestGenerator.fromCache(cache);
        CountingGenerator<Pair<Records.Request, ListenableFuture<O>>> operations = 
                CountingGenerator.create(iterations, iterations, 
                        SubmitGenerator.create(requests, cache), logger);
        while (operations.hasNext()) {
             Pair<Records.Request, ListenableFuture<O>> operation = operations.next();
             Operations.unlessError(operation.second().get(timeOut.value(), timeOut.unit()).record());
        }
        return cache;
    }

    protected final Logger logger = LogManager.getLogger(this);
    
    @Test(timeout=30000)
    public void testRandom() throws Exception {
        SimpleServerAndClient client = SimpleServerAndClient.defaults().setDefaults();
        ServiceMonitor monitor = client.getRuntimeModule().getServiceMonitor();
        for (Service service: client.build()) {
            monitor.add(service);
        }
        monitor.startAsync().awaitRunning();
        
        randomCache(
                100,
                TimeValue.milliseconds(1000),
                client.getClientBuilder().getConnectionClientExecutor(),
                logger);
        
        monitor.stopAsync().awaitTerminated();
    }
}
