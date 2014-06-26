package edu.uw.zookeeper.client;

import static org.junit.Assert.*;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.RandomSingleClientTest;
import edu.uw.zookeeper.SimpleServerAndClient;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.ZNodeCache;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;

@RunWith(JUnit4.class)
public class DeleteSubtreeTest {

    protected final Logger logger = LogManager.getLogger(this);
    
    @Test(timeout=30000)
    public void testRandom() throws Exception {
        SimpleServerAndClient client = SimpleServerAndClient.defaults().setDefaults();
        ServiceMonitor monitor = client.getRuntimeModule().getServiceMonitor();
        for (Service service: client.build()) {
            monitor.add(service);
        }
        monitor.startAsync().awaitRunning();
        
        LockableZNodeCache<ZNodeCache.SimpleCacheNode, Operation.Request, Message.ServerResponse<?>> cache = 
                RandomSingleClientTest.randomCache(
                        100, 
                        TimeValue.milliseconds(5000), 
                        client.getClientBuilder().getConnectionClientExecutor(), 
                        logger);
        
        Set<AbsoluteZNodePath> children = ImmutableSet.copyOf(
                Iterables.transform(
                        cache.cache().root().keySet(),
                        new Function<ZNodeName,AbsoluteZNodePath>() {
                            @Override
                            public AbsoluteZNodePath apply(ZNodeName input) {
                                return (AbsoluteZNodePath) ZNodePath.root().join(input);
                            }
                        }));
        assertEquals(children, ImmutableSet.copyOf(DeleteSubtree.deleteChildren(ZNodePath.root(), cache).get()));
        assertTrue(cache.cache().isEmpty());
        
        monitor.stopAsync().awaitTerminated();
    }
}
